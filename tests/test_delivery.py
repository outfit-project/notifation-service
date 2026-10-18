import asyncio
from email import policy
from email.parser import BytesParser

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.email.smtp_sender import SmtpSender, render_message
from app.consumer.stream_consumer import MailWorker
from app.main import create_app


def settings(**values):
    return Settings(_env_file=None, **values)


@pytest.mark.parametrize('email,code', [('bad', '123456'), ('a@b.com\r\nBcc: x@y.com', '123456'), ('a@b.com', '<script>'), ('a@b.com', '12\n34'), ('пользователь@example.com', '123456')])
def test_rejects_malformed_recipient_and_code(email, code):
    with pytest.raises(ValueError):
        render_message(settings(), {'type': 'verify_code', 'email': email, 'code': code})


def test_render_has_text_and_html_and_code():
    message = render_message(settings(), {'type': 'verify_code', 'email': 'alice@example.com', 'code': '012345'})
    assert message['To'] == 'alice@example.com'
    assert message.is_multipart()
    assert '012345' in message.get_body(preferencelist=('plain',)).get_content()
    assert '012345' in message.get_body(preferencelist=('html',)).get_content()


@pytest.mark.parametrize('values', [{'SMTP_STARTTLS': True, 'SMTP_USE_TLS': True}, {'SMTP_PASSWORD': 'secret'}, {'MAIL_FROM': 'evil\r\nBcc: x@y.com'}])
def test_rejects_unsafe_smtp_configuration(values):
    with pytest.raises(ValueError):
        settings(**values)


@pytest.mark.asyncio
@pytest.mark.parametrize('recipient,expected', [('alice@example.com', 'alice@example.com'), ('user@пример.рф', 'user@xn--e1afmkfd.xn--p1ai'), ('user@xn--e1afmkfd.xn--p1ai', 'user@xn--e1afmkfd.xn--p1ai'), ('user@faß.de', 'user@xn--fa-hia.de'), ('user@xn--fa-hia.de', 'user@xn--fa-hia.de')])
async def test_delivers_real_smtp_mime_message(recipient, expected):
    received = asyncio.Future()
    envelopes = []

    async def smtp(reader, writer):
        writer.write(b'220 localhost test SMTP\r\n')
        while line := await reader.readline():
            command = line.upper()
            if command.startswith(b'DATA'):
                writer.write(b'354 Send message\r\n')
                await writer.drain()
                lines = []
                while (line := await reader.readline()) != b'.\r\n':
                    if not line:
                        break
                    lines.append(line)
                received.set_result(b''.join(lines))
                writer.write(b'250 OK\r\n')
            elif command.startswith(b'QUIT'):
                writer.write(b'221 Bye\r\n')
                await writer.drain()
                break
            elif command.startswith(b'RCPT TO:'):
                envelopes.append(line.decode('ascii').strip())
                writer.write(b'250 OK\r\n')
            else:
                writer.write(b'250 OK\r\n')
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(smtp, '127.0.0.1', 0)
    async with server:
        sender = SmtpSender(settings(SMTP_HOST='127.0.0.1', SMTP_PORT=server.sockets[0].getsockname()[1]))
        await sender.send({'type': 'verify_code', 'email': recipient, 'code': '012345'})
        message = BytesParser(policy=policy.default).parsebytes(await asyncio.wait_for(received, 2))
        assert message['To'] == expected
        assert envelopes == [f'rcpt TO:<{expected}>']
        assert '012345' in message.get_body(preferencelist=('plain',)).get_content()


class Sender:
    def __init__(self, failing=False):
        self.failing = failing
        self.delivered = []

    async def send(self, event):
        if self.failing:
            raise OSError('SMTP unavailable')
        self.delivered.append(event)


@pytest.mark.asyncio
async def test_malformed_message_is_dead_lettered_and_next_message_delivered():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cfg = settings()
    sender = Sender()
    worker = MailWorker(redis, sender, cfg)
    await worker.ensure_group()
    await redis.xadd(cfg.MAIL_STREAM, {'type': 'unexpected'})
    await redis.xadd(cfg.MAIL_STREAM, {'type': 'verify_code', 'email': 'alice@example.com', 'code': '123456'})
    await worker.consume_once(block_ms=1)
    await worker.consume_once(block_ms=1)
    assert len(sender.delivered) == 1
    assert (await redis.xpending(cfg.MAIL_STREAM, cfg.MAIL_GROUP))['pending'] == 0
    dead = await redis.xrange(worker.dead_stream)
    assert len(dead) == 1
    assert dead[0][1]['reason'] == 'invalid_event'
    await redis.aclose()


@pytest.mark.asyncio
async def test_failed_smtp_is_pending_until_recovered_then_acknowledged():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cfg = settings(MAIL_RETRY_IDLE_MS=1)
    sender = Sender(failing=True)
    worker = MailWorker(redis, sender, cfg)
    await worker.ensure_group()
    await redis.xadd(cfg.MAIL_STREAM, {'type': 'verify_code', 'email': 'alice@example.com', 'code': '123456'})
    await worker.consume_once(block_ms=1)
    assert (await redis.xpending(cfg.MAIL_STREAM, cfg.MAIL_GROUP))['pending'] == 1
    sender.failing = False
    await asyncio.sleep(.02)
    restarted = MailWorker(redis, sender, settings(MAIL_CONSUMER='replacement', MAIL_RETRY_IDLE_MS=1))
    await restarted.recover_once()
    assert len(sender.delivered) == 1
    assert (await redis.xpending(cfg.MAIL_STREAM, cfg.MAIL_GROUP))['pending'] == 0
    await redis.aclose()


@pytest.mark.asyncio
async def test_bounded_retries_dead_letter_without_leaking_code():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    cfg = settings(MAIL_RETRY_IDLE_MS=1, MAIL_MAX_ATTEMPTS=2)
    worker = MailWorker(redis, Sender(failing=True), cfg)
    await worker.ensure_group()
    await redis.xadd(cfg.MAIL_STREAM, {'type': 'verify_code', 'email': 'alice@example.com', 'code': '123456'})
    await worker.consume_once(block_ms=1)
    await asyncio.sleep(.02)
    await worker.recover_once()
    assert (await redis.xpending(cfg.MAIL_STREAM, cfg.MAIL_GROUP))['pending'] == 0
    dead = await redis.xrange(worker.dead_stream)
    assert len(dead) == 1
    assert dead[0][1]['reason'] == 'delivery_failed'
    assert '123456' not in str(dead)
    await redis.aclose()


def test_lifespan_runs_worker_and_health_endpoints():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    app = create_app(settings(), redis_client=redis, sender=Sender())
    with TestClient(app) as client:
        assert client.get('/health/live').status_code == 200
        assert client.get('/health/ready').status_code == 200
    assert app.state.worker_task.done()


@pytest.mark.asyncio
async def test_only_active_delivery_is_claimed_to_avoid_batch_reclaim_duplicates():
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowSender:
        async def send(self, event):
            entered.set()
            await release.wait()

    cfg = settings()
    worker = MailWorker(redis, SlowSender(), cfg)
    await worker.ensure_group()
    for _ in range(3):
        await redis.xadd(cfg.MAIL_STREAM, {'type': 'verify_code', 'email': 'alice@example.com', 'code': '123456'})
    task = asyncio.create_task(worker.consume_once(block_ms=1))
    await asyncio.wait_for(entered.wait(), 1)
    pending = await redis.xpending(cfg.MAIL_STREAM, cfg.MAIL_GROUP)
    release.set()
    await task
    assert pending['pending'] == 1
    await redis.aclose()
