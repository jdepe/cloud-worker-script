const AWS = require('aws-sdk');
const axios = require('axios');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  sessionToken: process.env.AWS_SESSION_TOKEN,
  region: "ap-southeast-2",
});

const sqs = new AWS.SQS();
const s3 = new AWS.S3();

const queueName = 'n11069449-sqs-queue';
let queueUrl;

async function getQueueUrl() {
  const params = {
    QueueName: queueName
  };

  return new Promise((resolve, reject) => {
    sqs.getQueueUrl(params, (err, data) => {
      if (err) {
        console.error("Error fetching queue URL:", err);
        reject(err);
      } else {
        resolve(data.QueueUrl);
      }
    });
  });
}

async function pollQueue() {
  if (!queueUrl) {
    queueUrl = await getQueueUrl();
  }

  const params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 5, // Fetch up to 5 messages at a time
    WaitTimeSeconds: 20 // Long polling
  };

  return new Promise((resolve, reject) => {
    sqs.receiveMessage(params, (err, data) => {
      if (err) {
        console.error("Receive Error", err);
        return reject(err);
      }
      if (data.Messages) {
        resolve(data.Messages);
      } else {
        resolve([]);
      }
    });
  });
}

async function processMessage(message, receiptHandle) {
  // Download, compress then upload the file back to S3
  // After successful operation, delete the message from the queue
  console.log('Message found. Processing now...');
  const { folderKey, fileKeys, uniqueId } = message;
  const tempFolderBase = `/temp/${uniqueId}`;
  const tempFolderName = path.join(tempFolderBase, folderKey);

  fs.mkdirSync(tempFolderName, { recursive: true });

  // Download file from S3
  console.log('Downloading files from S3...');
  const downloadPromises = fileKeys.map(async (fileKey) => {
    const tempFileName = path.join(tempFolderName, path.basename(fileKey));

    const s3Params = {
      Bucket: 'n11069449-compress-store',
      Key: fileKey
    };

    const writeStream = fs.createWriteStream(tempFileName);

    return new Promise((resolve, reject) => {
      s3.getObject(s3Params)
        .createReadStream()
        .pipe(writeStream)
        .on('finish', resolve)
        .on('error', reject);
    });
  });


  await Promise.all(downloadPromises);
  console.log('Download complete.');

  // Compress file using pigz
  console.log('Compressing downloaded files...');
  const compressedFolderName = `${tempFolderBase}/${folderKey}.tar.gz`;

  await new Promise((resolve, reject) => {
    exec(`tar -cf - -C "${path.dirname(tempFolderName)}" "${path.basename(tempFolderName)}" | pigz -9 > "${compressedFolderName}"`, (error) => {
      if (error) {
        console.error(`Compression Error: ${error}`);
        return reject(error);
      }
      resolve();
    });
  });
  console.log('Compression complete');

  // Upload compressed file back to S3
  console.log('Uploading files back to S3...');
  const compressedReadStream = fs.createReadStream(compressedFolderName);
  console.log(folderKey);
  const uploadParams = {
    Bucket: 'n11069449-compress-store',
    Key: `${uniqueId}/${folderKey}.tar.gz`,
    Body: compressedReadStream
  };

  await new Promise((resolve, reject) => {
    s3.upload(uploadParams, (err, data) => {
      if (err) {
        console.error(`Upload Error: ${err}`);
        return reject(err);
      }
      console.log(`Upload Success: ${data.Location}`);
      resolve();
    });
  });
  console.log('Upload complete');

  // Delete message from queue
  console.log('Deleting message from queue and local files...');
  const deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: receiptHandle
  };

  sqs.deleteMessage(deleteParams, (err, data) => {
    if (err) {
      console.log("Delete Error", err);
    } else {
      // console.log("Message Deleted", data);
      console.log('Message deleted');
    }
  });

  // Delete locally stored files
  try {
    fs.rmSync(tempFolderBase, { recursive: true, force: true });
    console.log('Local files deleted.');
  } catch (err) {
    console.error(`Error deleting temporary folder: ${err}`);
  }


  console.log('Processing is complete.');
}

async function waitForQueue() {
  while (true) {
    try {
      const url = await getQueueUrl();
      console.log(`Queue found: ${url}`);
      return url;
    } catch (error) {
      if (error.code === 'AWS.SimpleQueueService.NonExistentQueue') {
        console.log(`Queue not found. Retrying in 5 seconds...`);
      } else {
        console.error(`Error fetching queue URL: ${error}`);
        console.log(`Unexpected error. Retrying in 5 seconds...`);
      }

      // Wait 5 seconds before trying
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

async function startWorker() {
  console.log(`Worker started`);
  queueUrl = await waitForQueue();
  while (true) {
    try {
      console.log(`Worker polling for messages...`);
      const messages = await pollQueue();
      if (messages.length > 0) {
        console.log(`Worker received ${messages.length} messages. Processing...`);
        // Process all messages concurrently
        await Promise.all(messages.map(message => {
          const body = JSON.parse(message.Body);
          const receiptHandle = message.ReceiptHandle;
          return processMessage(body, receiptHandle);
        }));
      }
    } catch (error) {
      console.error(`Worker encountered an error:`, error);
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

startWorker().catch(error => {
  console.error('Worker failed to start:', error);
})

