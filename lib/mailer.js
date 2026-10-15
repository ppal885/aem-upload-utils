/**
 * Optional outgoing mail for password-reset links. Set SMTP_HOST (plus SMTP_PORT, SMTP_USER, SMTP_PASS,
 * SMTP_SECURE, MAIL_FROM) to turn it on; without it, reset requests are shown to admins instead.
 */
const nodemailer = require('nodemailer');

const SMTP_HOST = process.env.SMTP_HOST || '';
const MAIL_FROM = process.env.MAIL_FROM || 'Guides Upload Utility <no-reply@adobe.com>';

let transport = null;
if (SMTP_HOST) {
    transport = nodemailer.createTransport({
        host: SMTP_HOST,
        port: parseInt(process.env.SMTP_PORT, 10) || 587,
        secure: /^(1|true|yes)$/i.test(process.env.SMTP_SECURE || ''),
        auth: process.env.SMTP_USER ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS || '' } : undefined,
    });
}

function mailEnabled() {
    return !!transport;
}

async function sendPasswordReset(to, name, link) {
    if (!transport) return false;
    await transport.sendMail({
        from: MAIL_FROM,
        to,
        subject: 'Reset your Guides Upload Utility password',
        text: `Hi ${name},\n\nSomeone (hopefully you) asked to reset your Guides Upload Utility password.\n`
            + `Open this link within 1 hour to choose a new one:\n\n${link}\n\n`
            + 'If you did not ask for this, ignore this email; your password stays the same.\n',
    });
    return true;
}

module.exports = { mailEnabled, sendPasswordReset };
