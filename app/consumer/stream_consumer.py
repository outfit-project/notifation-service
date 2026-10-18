import asyncio
import logging

from redis.exceptions import ResponseError

from app.core.config import Settings
from app.email.smtp_sender import validate_event

logger = logging.getLogger(__name__)


class MailWorker:
    def __init__(self, redis, sender, settings: Settings):
        self.redis = redis
        self.sender = sender
        self.settings = settings
        self.dead_stream = f'{settings.MAIL_STREAM}:dead'
        self.attempts_key = f'{settings.MAIL_STREAM}:{settings.MAIL_GROUP}:attempts'
        self.stop = asyncio.Event()
        self.initialized = False
        self.recovery_cursor = '0-0'

    async def ensure_group(self):
        try:
            await self.redis.xgroup_create(self.settings.MAIL_STREAM, self.settings.MAIL_GROUP, id='0', mkstream=True)
        except ResponseError as error:
            if 'BUSYGROUP' not in str(error):
                raise
        self.initialized = True

    async def _finish(self, message_id, reason=None, attempts=0):
        cfg = self.settings
        async with self.redis.pipeline(transaction=True) as pipeline:
            if reason:
                pipeline.xadd(self.dead_stream, {'source_id': message_id, 'reason': reason, 'attempts': str(attempts), 'type': 'verify_code'})
            pipeline.xack(cfg.MAIL_STREAM, cfg.MAIL_GROUP, message_id)
            pipeline.hdel(self.attempts_key, message_id)
            await pipeline.execute()

    async def process(self, message_id, event):
        try:
            validate_event(event)
        except (ValueError, TypeError):
            await self._finish(message_id, 'invalid_event')
            logger.warning('Invalid mail event moved to dead-letter stream: id=%s', message_id)
            return
        attempt = await self.redis.hincrby(self.attempts_key, message_id, 1)
        if attempt > self.settings.MAIL_MAX_ATTEMPTS:
            await self._finish(message_id, 'delivery_failed', attempt - 1)
            return
        try:
            await self.sender.send(event)
        except Exception as error:
            logger.warning('SMTP delivery failed: id=%s attempt=%s error_type=%s', message_id, attempt, type(error).__name__)
            if attempt >= self.settings.MAIL_MAX_ATTEMPTS:
                await self._finish(message_id, 'delivery_failed', attempt)
            return
        await self._finish(message_id)

    async def consume_once(self, block_ms=1000):
        cfg = self.settings
        batches = await self.redis.xreadgroup(cfg.MAIL_GROUP, cfg.MAIL_CONSUMER, {cfg.MAIL_STREAM: '>'}, count=1, block=block_ms)
        for _, messages in batches:
            for message_id, event in messages:
                if self.stop.is_set():
                    return
                await self.process(message_id, event)

    async def recover_once(self):
        cfg = self.settings
        result = await self.redis.xautoclaim(cfg.MAIL_STREAM, cfg.MAIL_GROUP, cfg.MAIL_CONSUMER, cfg.MAIL_RETRY_IDLE_MS, start_id=self.recovery_cursor, count=1)
        self.recovery_cursor = result[0]
        for message_id, event in result[1]:
            if self.stop.is_set():
                return
            await self.process(message_id, event)
        if len(result) > 2 and result[2]:
            await self.redis.hdel(self.attempts_key, *result[2])

    async def run(self):
        while not self.stop.is_set():
            try:
                if not self.initialized:
                    await self.ensure_group()
                await self.recover_once()
                await self.consume_once()
                await asyncio.sleep(0.01)
            except Exception as error:
                self.initialized = False
                logger.warning('Mail worker unavailable: error_type=%s', type(error).__name__)
                try:
                    await asyncio.wait_for(self.stop.wait(), timeout=3)
                except asyncio.TimeoutError:
                    pass
