# Notification service

Consumes auth verification events from a Redis stream and delivers text/HTML email through SMTP. A FastAPI lifespan runs the consumer; its HTTP surface contains health endpoints only.

[Auth producer](https://github.com/outfit-project/auth-service) · [Run all services](https://github.com/outfit-project/api-gateway#run-all-repositories)

## Start this repository

Requirements: Docker with Compose 2.20+. From this repository:

```bash
cp .env.example .env
docker network create vestis-services
docker compose up --build -d --wait --wait-timeout 120
```

PowerShell also supports `cp`. Copy the example only for initial setup; reconcile an existing `.env` instead of replacing it. Create the shared network once; skip creation if it already exists. `VESTIS_NETWORK` can select another pre-created network.

This starts notification, its own persistent Redis and Mailpit without any parent files or other checkouts. Open `http://localhost:8004/health/ready` and the inbox at `http://localhost:8025`. With the default empty Redis, the worker waits for events. `docker compose down` preserves mail and queue data.

To deliver auth registration codes, start auth on the same network, change notification's `.env` to `REDIS_URL=redis://vestis-auth-redis:6379/0`, match auth's `MAIL_STREAM`, and run `docker compose up -d`. The local Redis remains available for isolated tests, but connected mode consumes auth's queue. Mailpit captures mail locally; it does not forward messages to a real inbox.

The notification API is available to the gateway at `notification:8000`. `NOTIFICATION_PORT` and `MAILPIT_PORT` change the host ports if needed.

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
| `REDIS_URL` | Isolated: `redis://notification-redis:6379/0`; with auth: `redis://vestis-auth-redis:6379/0` |
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

The repository's `.env` configures the explicit environment in its own Compose file. SMTP and retry settings in the table are forwarded. For a fixed `MAIL_CONSUMER`, add it to the service environment; otherwise each instance generates its own identity.

## Run and inspect

From this repository:

```bash
docker compose ps
docker compose logs -f notification
curl http://localhost:8004/health/ready
docker compose exec -T notification-redis redis-cli XPENDING mail:outbox notifications
docker compose exec -T notification-redis redis-cli XLEN mail:outbox:dead
```

The Redis commands above inspect isolated mode. For connected mode run the corresponding commands against `auth-redis` from the auth repository. Open Mailpit, then register through auth or the web app to generate mail. Inspect dead-letter reasons with `XRANGE mail:outbox:dead - + COUNT 10`; source-stream contents contain verification codes and recipients.

To exercise isolated delivery with a synthetic event (Bash or PowerShell):

```bash
docker compose exec -T notification-redis redis-cli XADD mail:outbox '*' type verify_code email demo@example.com code 123456
```

The message should appear in Mailpit. This synthetic code is not an auth registration code.

Uvicorn listens internally on port 8000. `GET /health/live` checks the HTTP process. `GET /health/ready` checks Redis, consumer initialization and a live worker task. SMTP availability is excluded, allowing retries during provider outages. On shutdown the consumer stops reading and waits for active delivery before closing Redis.

For native development, configure host-reachable Redis/SMTP addresses, then run `uvicorn app.main:app --host 0.0.0.0 --port 8000` here. Compose does not publish Redis/SMTP ports to the host.

## Checks

In an isolated Python 3.12 environment, from this repository:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m compileall -q app
```

Tests use fakeredis for stream semantics and a real localhost SMTP server for MIME delivery. They cover malformed events, persistent retries, reclaim, readiness and internationalized recipients. External credentials are not needed.
