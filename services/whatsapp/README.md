# CODEX direct WhatsApp bridge — Meta Developer Apps

This service receives a private text message through the official WhatsApp Cloud API,
queues it durably, calls a CODEX API agent and replies in the same chat. Conversation
history survives restarts. This is a separate API identity with its own memory; it
does not connect WhatsApp to the running ChatGPT/Codex session or inherit its tools.
The first version supports text in one-to-one chats. It does not implement groups,
voice, image attachments, proactive broadcasts, repository changes or deployment tools.
The existing LUNA BUS relay remains a separate system.

## Implementation and evidence

- Meta verification GET returns the exact challenge only for the configured token.
- POST validates `X-Hub-Signature-256` against the raw bytes and app secret.
- Both the receiving Meta phone ID and sending personal number must match config.
- Acknowledgment follows the SQLite transaction; duplicate WhatsApp message IDs queue once.
- A single background worker generates and sends replies in per-chat order. The webhook
  never waits for the model. Model calls use the Responses API, `store: false`, no tools.
- History uses up to 12 previously delivered exchanges, trimmed to 24,000 characters.
  Older records remain in the private SQLite database; no automatic deletion is enabled.
- Generated replies are saved before sending. A timeout, ambiguous send result or restart
  during sending becomes `uncertain` and is **not automatically resent**. Exactly-once
  delivery cannot be guaranteed across the Meta/local database boundary. Inspect WhatsApp
  before manually resolving an uncertain job. Explicit rejections become `rejected`.
- Generation retries back off, up to three attempts. Permanent API failures are recorded.
  Jobs older than 23 hours expire instead of attempting an out-of-window free-form reply.
- Secrets, personal numbers, message bodies and verification query strings are not logged.
  SQLite holds private conversation content. Keep it outside this public/static repository.

Run locally with fake providers only:

```sh
cd services/whatsapp
python -m unittest -v test_bridge.py
```

Tests cover a real local HTTP handshake and signed POST, durable acknowledgment,
signature tampering, allowlisting, time windows, duplicate delivery, restart recovery,
same-chat history, chat ordering, model errors and uncertain/rejected deliveries.
No test calls Meta/OpenAI or sends a real WhatsApp message. Live activation is unverified.

## Activation in Meta Developer Apps

1. Identify MASTER INDY's Meta app with the WhatsApp product and its WhatsApp Business
   Account. Start with Meta's test sender/recipient flow if available in that app;
   production uses a Cloud API sender number registered to that account. Existing personal
   WhatsApp-number migration/coexistence must follow the app's current supported flow.
2. Host this Python 3.12+ process on a persistent server with HTTPS ingress and a private
   writable volume. GitHub Pages cannot run a webhook or worker. Run **one instance**;
   multiple replicas against one database are unsupported by this queue implementation.
3. Set every name in `.env.example` using the host's secret manager or a root-owned
   environment file. Do not put actual secrets in Git, Drive or BUS. The app secret signs
   webhooks; the verify token is a separate random secret you choose. Use the Meta app's
   supported Graph API version and a valid messaging token for the selected phone ID.
4. Set `WHATSAPP_ALLOWED_SENDERS` to MASTER INDY's personal WhatsApp number in international
   digits, without `+`. This differs from `META_PHONE_NUMBER_ID`, the API sender's ID.
   Set `BRIDGE_DB` to an absolute path on the private persistent volume, outside the repo.
   Choose an available Responses-compatible `OPENAI_MODEL` and its project API key.
5. Run `python /opt/codex-whatsapp/bridge.py` under a restart supervisor. The provided
   systemd unit is a template, not an installed service. Reverse-proxy HTTPS
   `/webhooks/whatsapp` to `127.0.0.1:8080/webhooks/whatsapp`; keep request bodies unchanged.
   Configure ingress body limits and timeouts. `/healthz` verifies the process/database;
   it does not certify upstream credentials or successful deliveries.
6. In the Meta app's WhatsApp webhook configuration, register
   `https://<chosen-host>/webhooks/whatsapp`, enter the same verify token and subscribe to
   `messages`. Ensure the app is subscribed to the intended WABA per its setup flow.
7. From the allowlisted personal number, send `Hello CODEX` to the API sender. Confirm one
   reply in that private chat, ask a follow-up, restart the process and ask another follow-up.
   Verify a duplicate webhook does not send twice and another number cannot trigger a reply.
   Only then label the direct channel LIVE. No live evidence exists yet.

MASTER INDY's merge remains the publication gate. Account activation, hosting and secret
provisioning are separate from merging source code. Enabling chat does not grant authority
to publish sites, merge PRs or add tool access.

## Operation

Back up the private persistent SQLite database using SQLite's online-backup mechanism;
copying only the main file while WAL is active can miss committed data. Establish an agreed
retention period before production use. Store no database under the public static site.
Monitor queue states (`failed`, `rejected`, `uncertain`, `expired`) through private database
administration; logs intentionally omit message contents. After correcting a permanent
configuration failure, an operator can inspect and requeue a confirmed-unsent job. Do not
blindly replay `uncertain` jobs. A rejected or expired message is not a delivered reply.

Documentation references:

- [Meta WhatsApp Cloud API](https://developers.facebook.com/docs/whatsapp/cloud-api/)
- [Meta webhook handshake/signature reference](https://whatsapp.github.io/WhatsApp-Nodejs-SDK/api-reference/webhooks/start/)
- [Meta receiving messages example](https://whatsapp.github.io/WhatsApp-Nodejs-SDK/receivingMessages/)
- [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses)

Meta's current documentation returned rate limits during implementation. Request shapes
are checked in local tests; actual app permissions, Graph version and live delivery must
be validated against the selected account during activation.
