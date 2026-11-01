# QPW BUS reliability and concise LDI task packets

Status: **review candidate, not activated**. Implements MASTER INDY's request to
assist THALEIA's five-part proposal while she is usage-capped. Supabase remains
the canonical ledger. Existing node identities, authorization ledger, Drive Docs
and old RPCs are not replaced, deleted or modified by this change.

LDIs start with [the one-page quickstart](LDI-QUICKSTART.md). New optional `work`
fields make Do / Deliver / Constraints / Evidence / Reply needed explicit. `brief`
views use deterministic budgets, shared artifact tables, explicit local view
receipts and on-demand originals; no model summarization or model call is needed.

## What is ready

| Proposal | Implementation / limit |
|---|---|
| Two-way Drive sync | Tested atomic local-spool engine with idempotent imports, bounded retries and explicit shared-only exports. LUNA's actual cloud Drive watcher/producer-owner validation must wrap it; this PR does **not** claim a running Drive daemon. |
| Atomic JSON envelopes | Strict v0.3 candidate contract, immutable IDs, checksummed artifact references, CLI compose/check/send/verify, private-by-default visibility. Legacy Docs remain available; prose is not automatically parsed into executable work. |
| Complete main snapshot | Deterministic archive of **every tracked Git blob**, including off-season/off-season.json, plus per-file hashes and exact commit in SNAPSHOT.json. First full archive is already staged in Drive/index. No new CI credential is required for the operator-run tool. |
| “Check the bus” shortcut | Short receiver procedure below. It is a workflow contract, not a promise that an idle ChatGPT/Gemini session can wake itself. |
| Explicit action classes | AUTO_PEER, NEEDS_RON_MERGE, NEEDS_RON_DECISION metadata. Labels route work; **they never grant approval**. Existing Needs You UI and authorization ledger are unchanged. |

The deployable LUNA runtime patch reuses her shared **authd surrogate exactly**:
`add_surrogate_to_request`, credential ID `custom.supabase`, original allowed host.
No credential values, node-config files, service keys or replacement authentication
scheme were read or added. `upstream/` preserves the two exact source files shared
by LUNA; `runtime/` contains the patched versions for a clear before/after review.

## Concrete defects found and repaired

1. **Node RPC marks unread mail delivered.** Live `ldi_inbox(p_limit)` returns a
   limited batch, then updates *all* waiting rows for the node. Rows outside the
   batch can disappear from later reads. Candidate `ldi_inbox_v2` is read-only;
   only an explicit per-message acknowledgment changes a receipt.
2. **Node send has no stable retry key.** Live `ldi_send` generates a new message ID
   per call. Candidate `ldi_send_v2` binds sender to the existing node token,
   preserves the caller's durable ID, accepts exact replays and rejects changed
   content under an existing ID. It never falls back to the old send RPC.
3. **LUNA coordinator parses display text.** It formerly regex-parsed a formatted
   inbox, then made an extra GET for every body. `inbox --json` returns full rows
   in one request; topics containing `|` and long bodies stay intact. For N
   messages, the coordinator drops N extra GETs: inbox + N acks, rather than
   inbox + N GETs + N acks (plus its separate coordination-ledger read).
4. **Failed ack falsely succeeds.** The old coordinator ignored subprocess return
   code and acknowledgment count. It now checks both and retries failed receipts.
5. **Crash can erase work.** State previously wrote directly to the final file
   after acknowledging mail, and discarded entries beyond 200. Full message
   content now persists atomically before acknowledgment; unresolved tasks remain
   in state. A compact `pending` projection exposes at most 50 IDs, with the
   total and truncation flag. Old tasks are not silently deleted.

Transport receipt is not task completion. `delivered` means the local spool has
durably stored the envelope. `processed` must be sent explicitly after the task
is actually handled. The spool never executes code, changes a repo, approves a
request, publishes anything or posts empty acknowledgment messages.

## Contract and compatibility

Required fields: `schema_version: "0.3"`, `message_id`, `sender`, `recipient`,
`topic`, `summary`, `created_at` (UTC ISO8601), `requires_reply` (boolean), `gate`,
`visibility` (`private` or `shared`), `artifact_refs` (array).
Optional: `task_id`, `reply_to_message_id`, `work`. `work` has kind/evidence plus
do/deliver/blocked_by/constraints. Tasks require do+deliver; results require deliver;
blockers require the exact blocked_by input. Instructions are at most 300 characters
each, with at most six 160-character constraints. Old envelopes remain valid.
Names are canonical lowercase. IDs are
`BUS-YYYYMMDD-<UPPERCASE-NODE>-<UNIQUE-SUFFIX>`. Summary is at most 2,000 characters;
the entire envelope at most 32 KiB. Store the generated envelope **before** sending
and reuse it on retries, rather than composing a fresh ID.

Artifact kinds: drive_file, drive_doc, github_pr, github_commit, repo_snapshot.
Every reference carries kind/uri/label; files require a full SHA256, snapshots also
require a full base_commit. Optional version/drive_id preserve provenance. The
verifier checks files already downloaded through a trusted provider; it never
follows arbitrary artifact URLs or executes attachments.

Canonical legacy rows remain unchanged. New messages store the envelope in
`payload.envelope` and a signed, readable `payload.body` for existing readers.
Legacy prose is not exported to the link-readable drop by default. No attempt is
made to migrate or reinterpret historical approval claims.

## Existing LUNA surrogate runtime — first rollout

1. After peer review and MASTER INDY's merge, install the **whole services/bus
   directory** on LUNA's existing trusted VM. Keep the existing private state and
   authorization ledger, and back them up before switching the cron entry.
2. Review the `upstream/` versus `runtime/` diff. Run the patched coordinator with
   its sibling bus_api.py; `QPW_BUS_API` can select an explicit installed path.
   `QPW_BUS_STATE_DIR` can select the existing private state directory. Authd
   imports and credential attachment remain unchanged. Test in that VM before
   switching the standing cron. Do not run two coordinators against one state file.
3. Inspect the full `pending_count` and persistent task state. State persistence
   does not prove downstream action execution; the worker still needs to return
   a deliverable/finding/blocker and mark its task handled through the existing
   workflow. Do not infer completion from an ack.
4. New sends can use `runtime/bus_api.py send-envelope <file>` or `bus.py send`.
   These use PostgREST's conflict-ignore operation, then verify immutable content
   on a replay. The older `send` command remains compatible and is not given a
   silent non-idempotent fallback by the new client.
5. `BUS_NODE` selects the explicitly delegated **private runtime** spool identity.
   No secret environment variables are introduced for this surrogate path.
   The service-role adapter is not a public/client-facing identity boundary.

```sh
python bus.py compose --sender luna --recipient thaleia --topic "Review packet" \
  --summary "OBSERVED: packet is staged; verify its checksum before review." \
  --task-id BUS-OPTIMIZATION --requires-reply --outbox /private/spool/outgoing
python bus.py check /private/spool/outgoing/BUS-....json
BUS_NODE=luna python bus.py send /private/spool/outgoing/BUS-....json
```

Add `--artifact-refs /private/refs.json` for an array of durable references. Add
`--shared` only when the envelope itself is suitable for the shared Drive folder.
`shared` does not change any referenced file's ACL or publish its contents.

## Cloud Drive handoff — precise remaining integration dependency

The source share contains bus_api.py and bus-coordinator.py, **not** the persistent
Drive watcher/Google authorization adapter. A local directory is not Google Drive.
The code intentionally does not invent a Google credential ID, extract a token,
grant sharing permissions or deploy a second cloud daemon.

LUNA's watcher should download a complete, producer-authenticated JSON envelope
to the node's private `outgoing/` spool, then call:

```sh
BUS_NODE=thaleia python bus.py sync --spool /private/thaleia-spool --allow-import
```

Only enable import after validating the Drive producer's account/identity and
delegation. A sender name or a typed “— THALEIA” is not authentication. Private
spool permissions prevent arbitrary shared-folder writers from impersonating a
node. Group/world-writable or symlinked spool paths are rejected.

The engine sends each immutable ID at most once canonically (server dedupe), keeps
transport failures pending, preserves malformed files in dead-letter and leaves
source files intact. Upload `incoming/*.json` to the shared folder with a provider
operation that finalizes complete files atomically. Reserve a Drive file ID or
dedupe by ID+checksum after ambiguous uploads; name-only search is not sufficient.
Cloud delivery must have its own provider receipt after upload; the local receipt
does not certify a Drive upload. Existing INBOX/OUTBOX Docs stay as a readable
fallback until both directions have real end-to-end proof. No such proof exists yet.

## Additive node RPC migration — separate review/apply step

`sql/001_bus_v2.sql` is an **unapplied candidate**, verified using PostgreSQL compiled
to WASM (PGlite). It creates send/inbox/ack v2 and a per-recipient receipt table;
old RPC definitions and existing messages remain untouched. The new receipt table
has RLS and no direct anonymous access. New functions use the existing `_ldi_who`
verifier, a locked search path, sender binding and explicit recipient checks.
Broadcast receipts are per node, so one receiver cannot finish mail for everyone.

The candidate uses SECURITY DEFINER because that is the deployed node-token
boundary, not as a shortcut for public table access. A database/security peer must
review ownership/grants and the existing verifier before promotion. The public
transport key alone does not authorize a node. No SQL is auto-applied by CI or by
merging this repository. An existing node runtime can inject its current in-memory
credentials into the optional RPC adapter; this module never reads node-config
files or creates another credential store.

Pilot one node; retire its old reader before enabling the new one. No concurrent
workers for the same spool/state. At cutover, explicitly reconcile historical
pending rows. Do not bulk-ack unseen mail to make a backlog disappear. Task-side
effects still need idempotency: this provides idempotent transport, not a universal
exactly-once execution guarantee. Rollback routes the pilot back to its previous
reader; preserve receipt rows and envelope files for audit.

## Full main archive

```sh
git -C /path/to/this-week-in-aspen fetch origin
python snapshot.py build --repo /path/to/this-week-in-aspen --revision origin/main \
  --output /private/staging/this-week-in-aspen-main.zip
python bus.py verify /private/staging/this-week-in-aspen-main.zip <INDEX-SHA256>
python snapshot.py verify /private/staging/this-week-in-aspen-main.zip
```

The build reports exact commit, file census, bytes and outer SHA256. Only Git blobs
from that commit enter the archive; untracked files and working-tree .env files are
excluded. If credentials were committed to Git, stop and remediate before sharing;
this tool is not a secret scanner. Submodules require an explicit policy and fail
rather than producing an incomplete archive. The archive includes `SNAPSHOT.json`
with all per-file hashes/modes and the commit. Verify the outer checksum from the
trusted index **before** trusting the embedded manifest.

After each merge, update the same canonical Drive file ID, then update 0000 with
commit/SHA/census. That is an operator step in this pass; no automated post-merge
Drive uploader has been activated. Never label a stale archive “current main”
without its observed commit. The first snapshot is pinned to `589e226…`.

## Universal CHECK BUS // SYNC

“Check the bus” means: read 0000 first; read your authenticated inbox; compare
message IDs with durable task state; verify referenced payload hashes/base commits;
execute already-authorized work or return one exact blocker; post a compact signed
deliverable with task/reply IDs and artifact refs; record state. A pure receipt
does not satisfy an execution order. No empty agreement loops, raw history dumps,
automatic publication or invented approvals. Surface only decisions/merges that
actually need MASTER INDY. This runbook is ready for THALEIA's return; her platform's
saved workflow and LUNA's cron adoption remain their runtime integration steps.

## Validation / peer handoff

`python -m unittest -v test_bus.py test_brief.py` — 42 tests, including actual shared-script code,
budget/deferred delivery, original retention, replay, changed-message notices and typed work.
`npm ci --ignore-scripts && npm test` — 41 PostgreSQL contract/security/receipt checks.
No live credentials or production mutations occur in tests. PGlite is a real local
PostgreSQL engine, not the deployed Supabase runtime; live permissions/performance
still require a controlled pilot. CI runs both suites and does not deploy anything.

### Token/context behavior

`python bus.py brief --state /private/node/brief.sqlite` defaults to 6,000 **characters**,
not an assumed token count. A local private cache stores the original rows. Only
explicitly acknowledged view receipts suppress unchanged cards; deferred messages
are never acknowledged by that receipt. Changed full content, including changes
outside a visible preview, triggers a new notice. A stale receipt cannot hide a
newer message. No canonical task status or publication authority changes.

Use `--cache` for deferred cards, `--after` for the printed keyset server cursor,
`brief-show MESSAGE_ID --refresh` for current originals, and `--all` for a review
of seen pending work. Read-only brief generation is distinct from the existing
coordinator: `runtime/bus-coordinator.py --brief` still performs its established
durable local storage/transport acknowledgments before producing a bounded view.
Both modes retain full originals and explicitly label cached/coordinator state.
Default legacy coordinator JSON output is preserved until its caller opts in.

Structured cards retain full action/constraint fields. If a card cannot fit, the
brief emits a full-message-required stub; legacy prose is never guessed into a
task. Separate messages are not semantically collapsed by topic or task ID.

`benchmark-results.json` reports a private 20-message sample: 7,547 cl100k_base
reference tokens for raw rows, about 1,838 for the first brief (13 shown, seven
deferred), and 47 for an unchanged recheck after both packets were read. All 20
originals remain available. This is approximately **76% less first-triage input**,
not an end-to-end billing/cost guarantee; fetching full details costs extra and
different LDI models tokenize differently. Receipt IDs vary slightly between runs.

Reproduce against a private exported sample after `npm ci --ignore-scripts`:
`python benchmark.py /private/sample.json --output /private/aggregate-metrics.json`.
The script outputs aggregate metrics only. The pinned dev-only js-tiktoken package
bundles cl100k_base; it makes no model/network call. Private source messages and
benchmark cache files are not committed or uploaded with the implementation.

Sources: [Transport v0.1](https://docs.google.com/document/d/17N2IfT2yjJJKSU-nlcrkYB4otHBVOAplsK5QcVs_OJM/edit),
[approved protocol v0.2](https://drive.google.com/file/d/1UGr5iqh9-fwOdc2Khbf-4HzdvIplmD0C/view),
[CHECK BUS/SYNC](https://docs.google.com/document/d/1Gvuy19rEX-6tTiTR14v1_wj_AM_NUsrWciWVhYVQw9E/edit),
LUNA's [bus_api.py](https://drive.google.com/file/d/1ieKtzk7sJPOgbFOSi1U2ckjIBtpmJ0KI/view) and
[coordinator](https://drive.google.com/file/d/1eJVtLRDjCabmMRsoaho7jbAko5zPRR7G/view),
plus MASTER INDY's screenshots of THALEIA's five optimizations.

Remaining review: THALEIA refines the contract/Drive flow; HEPHAESTUS reviews the
transport/RPC boundary; NEXUS reviews retry/cutover semantics. The coordinator's
existing keyword-overlap authorization matcher is explicitly **unchanged** and is
not proof of permission; exact authorization-reference handling deserves a separate
governance review. No matcher label or envelope gate can authorize a publication.

— CODEX
