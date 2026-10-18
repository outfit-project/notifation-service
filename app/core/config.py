import socket
import uuid

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    REDIS_URL: str = 'redis://auth-redis:6379/0'
    MAIL_STREAM: str = 'mail:outbox'
    MAIL_GROUP: str = 'notification'
    MAIL_CONSUMER: str = Field(default_factory=lambda: f'{socket.gethostname()}-{uuid.uuid4().hex[:8]}')
    MAIL_MAX_ATTEMPTS: int = Field(default=5, ge=1, le=100)
    MAIL_RETRY_IDLE_MS: int = Field(default=60000, ge=1)
    SMTP_HOST: str = 'mailpit'
    SMTP_PORT: int = Field(default=1025, ge=1, le=65535)
    SMTP_USER: str = ''
    SMTP_PASSWORD: str = ''
    SMTP_STARTTLS: bool = False
    SMTP_USE_TLS: bool = False
    SMTP_TIMEOUT: float = Field(default=10, gt=0, le=60)
    MAIL_FROM: str = 'Vestis <noreply@vestis.local>'

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    @model_validator(mode='after')
    def validate_smtp(self):
        if self.SMTP_STARTTLS and self.SMTP_USE_TLS:
            raise ValueError('SMTP_STARTTLS and SMTP_USE_TLS are mutually exclusive')
        if bool(self.SMTP_USER) != bool(self.SMTP_PASSWORD):
            raise ValueError('SMTP_USER and SMTP_PASSWORD must be configured together')
        if any(ord(char) < 32 or ord(char) == 127 for char in self.MAIL_FROM):
            raise ValueError('MAIL_FROM cannot contain control characters')
        from email.utils import parseaddr
        address = parseaddr(self.MAIL_FROM)[1]
        if '@' not in address or not address.rsplit('@', 1)[1]:
            raise ValueError('MAIL_FROM must contain a valid sender address')
        return self
