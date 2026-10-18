# Notification service

Consumes auth verification events from a Redis stream and delivers text/HTML email through SMTP. A FastAPI lifespan runs the consumer; its HTTP surface contains health endpoints only.

[Project overview](../../README.md) · [Auth producer](../auth-service/README.md) · [SMTP deployment](../../DEPLOY.md)

## Event and delivery contract

Auth appends these string fields to `mail:outbox`:

```json
{"type": "verify_code", "email": "person@example.com", "code": "012345"}
```

Only `verify_code` events with a valid recipient and six ASCII digits are accepted. Recipient local parts must be ASCII. Internationalized domains use IDNA2008 with non-transitional UTS46 conversion, preserving distinctions such as `faß.de` rather than rewriting it to a different domain.

Each event becomes an `EmailMessage` with plain text and HTML alternatives, date and message ID. Blocking SMTP work runs in a thread. Credentials, recipients and verification codes are not included in worker error logs.

## Recovery and retry semantics

The consumer group starts at stream ID `0`, allowing existing events to be processed. `XREADGROUP` reads one new message at a time; `XAUTOCLAIM` traverses idle pending messages using a saved cursor. Each iteration can recover a pending event and then consume a new event, so a failed message does not prevent later mail from being attempted.

Attempts are recorded before delivery in Redis hash `<MAIL_STREAM>:<MAIL_GROUP>:attempts` and survive restarts. Default maximum: five SMTP attempts, including attempts interrupted by crashes. Failures remain pending until reclaim after `MAIL_RETRY_IDLE_MS`. Keep that idle period above worst-case SMTP delivery time to avoid concurrent claims. Redis outages pause the loop for three seconds.

Malformed events and exhausted retries are atomically appended to `<MAIL_STREAM>:dead`, acknowledged and removed from the attempt hash. Dead letters retain `source_id`, `reason`, `attempts` and the supported event type, omitting recipient/code. Source events remain in the original stream, subject to producer retention. Auth approximately caps that stream at 100,000 entries; retention should exceed the maximum acceptable pending-mail age.

Delivery is **at least once**: a crash after SMTP acceptance but before acknowledgment can send a duplicate. There is no automatic dead-letter replay API. Run one Uvicorn process per container; multiple containers must use the same group and distinct consumer identities. Changing the group creates an independent consumer group and can resend existing events.

## Configuration

Settings: [app/core/config.py](app/core/config.py), with a local [.env.example](.env.example). The table distinguishes Compose overrides from application defaults.

| Variable | Value / purpose |
| --- | --- |
| `REDIS_URL` | Compose: `redis://auth-redis:6379/0` |
| `MAIL_STREAM` | `mail:outbox`; same as auth |
| `MAIL_GROUP` | Compose: `notifications`; application default: `notification` |
| `MAIL_CONSUMER` | Generated hostname/UUID identity unless set |
| `MAIL_MAX_ATTEMPTS` | `5` |
| `MAIL_RETRY_IDLE_MS` | `60000` |
| `SMTP_HOST`, `SMTP_PORT` | `mailpit`, `1025` |
| `SMTP_USER`, `SMTP_PASSWORD` | Empty locally; must be supplied together |
| `SMTP_STARTTLS`, `SMTP_USE_TLS` | Both `false` locally; mutually exclusive |
| `SMTP_TIMEOUT` | Socket timeout, default `10` seconds |
| `MAIL_FROM` | Compose: `Vestis <hello@vestis.local>`; app default uses `noreply` |

For a real provider, use its authorized sender, host, port and credentials. Enable STARTTLS (often port 587) or implicit TLS (often 465) as required. TLS verifies certificates using system trust. Mailpit captures messages; it does not forward them to external recipients. External SMTP/TLS has not been exercised in the local verification record.

Root `.env` values are passed only where referenced by Compose. Add worker-specific tuning values to the service environment or an override.

## Run and inspect

From the project root:

```bash
python scripts/setup.py
docker compose up --build -d --wait --wait-timeout 900
docker compose logs -f notification
curl http://localhost:8080/health/notification
docker compose exec -T auth-redis redis-cli XPENDING mail:outbox notifications
docker compose exec -T auth-redis redis-cli XLEN mail:outbox:dead
```

Open the local inbox at `http://localhost:8025`, then register through the app to generate real email. Inspect dead-letter reasons locally with `XRANGE mail:outbox:dead - + COUNT 10`; avoid publishing source-stream contents, which contain verification codes and recipients.

Uvicorn listens internally on port 8000. `GET /health/live` checks the HTTP process. `GET /health/ready` checks Redis, consumer initialization and a live worker task. SMTP availability is excluded, allowing retries during provider outages. On shutdown the consumer stops reading and waits for active delivery before closing Redis.

For native development, configure host-reachable Redis/SMTP addresses, then run `uvicorn app.main:app --host 0.0.0.0 --port 8000` here. Root Compose does not publish Redis/SMTP ports to the host.

## Checks

In an isolated Python 3.12 environment, from the project root:

```bash
python -m pip install -r backend/notification-service/requirements.txt -r requirements-test.txt
cd backend/notification-service
python -m pytest -q
python -m compileall -q app
```

Tests use fakeredis for stream semantics and a real localhost SMTP server for MIME delivery. They cover malformed events, persistent retries, reclaim, readiness and internationalized recipients. External credentials are not needed.
