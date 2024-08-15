const DirectBinary = require('@adobe/aem-upload');

// URL to the folder in AEM where assets will be uploaded. Folder
// must already exist.
const targetUrl = 'https://author-p111055-e1081044.adobeaemcloud.com/content/dam/ibm';

// list of all local files that will be uploaded.
const uploadFiles = ['/Users/pulgupta/Library/CloudStorage/OneDrive-Adobe/Dox-Content/6k_files/6k_topics/gasb-qa_utf8'];

const upload = new DirectBinary.DirectBinaryUpload();
const options = new DirectBinary.DirectBinaryUploadOptions()
    .withUrl(targetUrl)
    .withUploadFiles(uploadFiles)
    .withMaxConcurrent(10);

upload.uploadFiles(options)
    .then(result => {
        console.log(result)
    })
    .catch(err => {
        console.error(err)
    });