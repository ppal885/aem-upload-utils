/**
 * The Authorization header for requests to AEM. An AEM as a Cloud Service access token (the JWT from
 * Developer Console > Integrations > Local token, or a service credential) entered in the password field
 * is sent as Bearer; anything else is a username and password, sent as Basic.
 */
const JWT = /^eyJ[\w-]+\.[\w-]+\.[\w-]*$/;

function isAccessToken(password) {
    return JWT.test(String(password || '').trim());
}

function aemAuthorization(username, password) {
    if (isAccessToken(password)) return 'Bearer ' + String(password).trim();
    return 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
}

module.exports = { isAccessToken, aemAuthorization };
