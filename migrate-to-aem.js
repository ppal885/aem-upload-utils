const {
    FileSystemUploadOptions,
    FileSystemUpload
} = require('@adobe/aem-upload');
const credentials = Buffer.from('testadmin:testadmin').toString('base64')
const options = new FileSystemUploadOptions()
    .withUrl('https://author-p111055-e1081044.adobeaemcloud.com/content/dam/ibm/4k')
    .withDeepUpload(true)
    .withMaxUploadFiles(50000)
    .withMaxConcurrent(20)
    .withHttpOptions({
        headers: {
            Authorization: `Basic ${credentials}`
        }
    });
async function run() {
    // upload a single asset and all assets in a given folder
    const fileUpload = new FileSystemUpload();
    await fileUpload.upload(options, [
        '/Users/pulgupta/Library/CloudStorage/OneDrive-Adobe/Dox-Content/4k_files/gasb-cig_utf8/gasb-cig_utf8'
    ]);
}
run();