import asyncio
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
import re
import smtplib
import ssl

import idna

from app.core.config import Settings


def validate_event(event: dict) -> str:
    email = event.get('email', '')
    code = event.get('code', '')
    if event.get('type') != 'verify_code':
        raise ValueError('Unsupported mail event')
    if not isinstance(email, str) or email.count('@') != 1:
        raise ValueError('Invalid recipient')
    local, domain = email.split('@')
    if len(local) > 64 or not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+", local):
        raise ValueError('Invalid recipient')
    try:
        domain = idna.encode(domain, uts46=True, std3_rules=True, transitional=False).decode('ascii').lower()
    except UnicodeError:
        raise ValueError('Invalid recipient domain') from None
    labels = domain.split('.')
    if len(labels) < 2 or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) for label in labels):
        raise ValueError('Invalid recipient domain')
    recipient = f'{local}@{domain}'
    if len(recipient) > 254:
        raise ValueError('Invalid recipient')
    if not isinstance(code, str) or not re.fullmatch(r'[0-9]{6}', code):
        raise ValueError('Invalid verification code')
    return recipient


def render_message(settings: Settings, event: dict) -> EmailMessage:
    recipient = validate_event(event)
    message = EmailMessage()
    message['From'] = settings.MAIL_FROM
    message['To'] = recipient
    message['Subject'] = 'Your Vestis verification code'
    message['Date'] = formatdate(localtime=False)
    message['Message-ID'] = make_msgid()
    code = event['code']
    message.set_content(f'Your Vestis verification code is {code}.\n\nIf you did not request this code, ignore this email.\n')
    message.add_alternative(f'<html><body><p>Your Vestis verification code:</p><p><strong>{code}</strong></p><p>If you did not request this code, ignore this email.</p></body></html>', subtype='html')
    return message


class SmtpSender:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def send(self, event: dict) -> None:
        message = render_message(self.settings, event)
        await asyncio.to_thread(self._send, message)

    def _send(self, message: EmailMessage) -> None:
        cfg = self.settings
        options = {'host': cfg.SMTP_HOST, 'port': cfg.SMTP_PORT, 'timeout': cfg.SMTP_TIMEOUT}
        if cfg.SMTP_USE_TLS:
            connection = smtplib.SMTP_SSL(**options, context=ssl.create_default_context())
        else:
            connection = smtplib.SMTP(**options)
        with connection as smtp:
            smtp.ehlo()
            if cfg.SMTP_STARTTLS:
                smtp.starttls(context=ssl.create_default_context())
                smtp.ehlo()
            if cfg.SMTP_USER:
                smtp.login(cfg.SMTP_USER, cfg.SMTP_PASSWORD)
            refused = smtp.send_message(message)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
