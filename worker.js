const AWS = require('aws-sdk');
const axios = require('axios');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');

AWS.config.update({ region: 'ap-southeast-2' });

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
    MaxNumberOfMessage: 1,
    WaitTimeSeconds: 20 // Long polling
  }

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

async function processMessage(message) {
  // Implement your logic to download, compress, and upload file
  // After successful operation, delete the message from the queue
  const folderKey = message.folderKey;
  const fileKeys = message.fileKeys;
  const tempFolderName = `/tmp/${folderKey}`;

  // Download file from S3
  fs.mkdirSync(tempFolderName, { recursive: true });

  const downloadPromises = fileKeys.map(async (fileKey) => {
    const downloadUrl = `https://n11069449-compress-store.s3.amazonaws.com/${fileKey}`;
    const response = await axios({
      url: downloadUrl,
      method: 'GET',
      responseType: 'stream'
    });

    const readStream = response.data;
    const tempFileName = path.join(tempFolderName, path.basename(fileKey));
    const writeStream = fs.createWriteStream(tempFileName);

    return new Promise((resolve, reject) => {
      readStream.pipe(writeStream);
      writeStream.on('finish', resolve);
      writeStream.on('error', reject);
    });
  });

  await Promise.all(downloadPromises);

  // Compress file using pigz
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

  // Upload compressed file back to S3
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

  // Delete message from queue
  const deleteParams = {
    QueueUrl: 'url',
    ReceiptHandle: receiptHandle
  };

  sqs.deleteMessage(deleteParams, (err, data) => {
    if (err) {
      console.log("Delete Error", err);
    } else {
      console.log("Message Deleted", data);
    }
  });
}

getQueueUrl().then(url => {
  queueUrl = url;
  setInterval(pollQueue, 3000);
}).catch(error => {
  console.error('Failed to get SQS queue URL', error);
});