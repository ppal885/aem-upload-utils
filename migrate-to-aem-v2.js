const DirectBinary = require('@adobe/aem-upload');
const fs = require('fs');
const {join} = require("node:path");

const targetUrl = 'https://author-p111055-e1081044.adobeaemcloud.com/content/dam/ibm';
const startingDirectory = '/Users/pulgupta/Library/CloudStorage/OneDrive-Adobe/Dox-Content/contract-tests/contract-tests';
const uploadFiles = [];

// Traverse files and prepare data for upload
function prepareFileData(dir) {
    const files = fs.readdirSync(dir);
    for (const file of files) {
        const filePath = join(dir, file);
        const fileStat = fs.statSync(filePath);
        if (fileStat.isDirectory()) {
            prepareFileData(filePath);
        } else if (file !== '.DS_Store') {
            uploadFiles.push({
                fileName: file,
                filePath: filePath,
                fileSize: fileStat.size
            });
        }
    }
}

prepareFileData(startingDirectory);
console.log("Uploading files...");

const upload = new DirectBinary.DirectBinaryUpload();
const options = new DirectBinary.DirectBinaryUploadOptions()
    .withUrl(targetUrl)
    .withUploadFiles(uploadFiles)
    .withMaxConcurrent(10)
    .withHttpOptions({
        headers: {
            Authorization: `Basic ${Buffer.from('testadmin:testadmin').toString('base64')}`
        }
    });

upload.on('filestart', data => {
    const { fileName } = data;
    console.log(`Started uploading ${fileName}`);
});
upload.on('fileprogress', data => {
    const { fileName, transferred } = data;
    console.log(`Uploading ${fileName}`)
});
upload.on('fileend', data => {
    const { fileName } = data;
    console.log(`Uploaded successfully ${fileName}`)
});
upload.on('fileerror', data => {
    const { fileName, errors } = data;
    console.log(`Error in uploading ${fileName}`)
});

upload.uploadFiles(options);