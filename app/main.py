import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from app.consumer.stream_consumer import MailWorker
from app.core.config import Settings
from app.email.smtp_sender import SmtpSender


def create_app(settings=None, *, redis_client=None, sender=None):
    cfg = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        redis = redis_client if redis_client is not None else Redis.from_url(cfg.REDIS_URL, decode_responses=True, socket_connect_timeout=3, socket_timeout=5)
        worker = MailWorker(redis, sender or SmtpSender(cfg), cfg)
        app.state.redis = redis
        app.state.worker = worker
        app.state.worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0)
        try:
            yield
        finally:
            worker.stop.set()
            try:
                await asyncio.wait_for(app.state.worker_task, timeout=cfg.SMTP_TIMEOUT * 5 + 2)
            except asyncio.TimeoutError:
                pass
            finally:
                await redis.aclose()

    app = FastAPI(title='Vestis notifications', lifespan=lifespan)

    @app.get('/health/live')
    async def live():
        return {'status': 'ok'}

    @app.get('/health/ready')
    async def ready():
        if app.state.worker_task.done() or not app.state.worker.initialized:
            raise HTTPException(status_code=503, detail='Mail consumer unavailable')
        try:
            await app.state.redis.ping()
        except Exception:
            raise HTTPException(status_code=503, detail='Redis unavailable') from None
        return {'status': 'ok'}

    return app


app = create_app()
