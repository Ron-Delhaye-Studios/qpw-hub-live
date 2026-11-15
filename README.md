# qpw-hub-live
QPW Hub static page (public shell, token-gated API)

## WO#5: primary Hub

Three primary sections: **Needs you · Active · Work orders**. Messages and sending
remain below the board. Use an existing private key directly; PIN login and optional
PIN setup remain available. Private keys stay in session storage and are cleared by
Lock. No private key or privileged database credential is shipped with the site.

The existing `qpw-hub-live` Edge Function is the sole data path. Reads refresh every
30 seconds while the page is visible and when returning to the page. The source
timestamp ages each second; failed reads, unknown timestamps and data older than
60 seconds are visibly stale. This establishes BUS read connectivity only; agent
connectivity remains unknown.

Needs you includes tasks explicitly owned by `master-indy` in blocked, approval,
or review states, plus approval/reply requests addressed to `master-indy`. The API
does not expose a distinct decision-owner field: an agent-owned blocked task is not
automatically an Indy decision. Work orders displays the returned task ledger,
including recorded evidence, verifier and verification date. The page does not
independently certify an artifact or convert replies into completion.

### Verification

Run `node --test tests/hub.test.cjs` (Node 22 or newer). These dependency-free tests
execute the actual page script against a simulated DOM and mocked fetch, including
auth rejection, lock races, stale timestamps, polling, unsafe text and retry keys.
They are behavioral checks, not browser layout or authenticated hosted acceptance.

### Known backend limits

- State returns the latest 100 tasks and 200 messages; this is not the entire
  unresolved queue. Messages displays the latest 25 of that response.
- Deployed v8 selects `body:payload->>body`, omitting other structured payload
  fields. The page preserves payloads if supplied and explicitly flags absent
  content. A separately reviewed backend fix is required to recover omitted data.
- Reading does not acknowledge messages. Peek/ack migration and runtime presence
  remain unverified. No backend/schema changes are included here.

### Hosted acceptance after approved merge

LUNA verifies on phone and MSI desktop: direct key unlock without PIN setup,
all three sections from real state, correct decision ownership, age/stale/recovery,
Lock and reload, and an authorized unchanged-message retry without duplication.
Verify one work order through a result with artifact reference/version and verifier.
If meaningful structured content is missing, hold that acceptance item for the
backend projection fix. Never call the Hub operational from the access screen alone.

Merge and publication require Master Indy's approval. This branch does not change
Pages configuration or deploy the Edge Function.
