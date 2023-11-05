const AWS = require('aws-sdk');
const axios = require('axios');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

// REMOVE BEFORE SUBMISSION (+ take out dotenv from package.json + package-lock.json)
require('dotenv').config();

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
        return reject(err);
      }
      resolve(data.QueueUrl);
    });
  });
}

async function pollQueue() {
  // Check if url is saved otherwise retrieve it
  if (!queueUrl) {
    queueUrl = await getQueueUrl();
  }

  const params = {
    QueueUrl: queueUrl, // Replace with SQS url
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20 // Long polling
  }
  console.log('Checking for message in queue...');
  sqs.receiveMessage(params, (err, data) => {
    if (err) {
      console.log("Receive Error", err);
    } else if (data.Messages) {
      const message = JSON.parse(data.Messages[0].Body);
      const receiptHandle = data.Messages[0].ReceiptHandle;
      processMessage(message, receiptHandle).catch(console.error);
    }
  });
}

async function processMessage(message, receiptHandle) {
  // Download, compress then upload the file back to S3
  // After successful operation, delete the message from the queue
  console.log('Message found. Processing now...');

  const folderKey = message.folderKey;
  const fileKeys = message.fileKeys;
  const tempFolderName = `/tmp/${folderKey}`;

  // Download file from S3
  console.log('Downloading files from S3...');
  fs.mkdirSync(tempFolderName, { recursive: true });

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
  const compressedFolderName = `${tempFolderName}.tar.gz`;

  await new Promise((resolve, reject) => {
    exec(`tar -cf - -C ${path.dirname(tempFolderName)} ${path.basename(tempFolderName)} | pigz -9 > ${compressedFolderName}`, (error) => {
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

  const uploadParams = {
    Bucket: 'n11069449-compress-store',
    Key: `${folderKey}.tar.gz`,
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
  fs.rmSync(tempFolderName, { recursive: true, force: true });
  console.log('Local files deleted.');
  console.log('Processing is complete.');
}

async function startWorker(workerId) {
  console.log(`Worker ${workerId} started`);
  while (true) {
    try {
      await pollQueue();
    } catch (error) {
      console.error(`Worker ${workerId} encountered an error:`, error);

      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

getQueueUrl().then(url => {
  queueUrl = url;
})
.catch(error => {
console.error('Failed to get SQS queue URL', error);
});

const maxWorkers = 5;

for (let i = 0; i < maxWorkers; i++) {
  startWorker(i).catch(error => {
    console.error(`Worker ${i} failed to start:`, error);
  });
}