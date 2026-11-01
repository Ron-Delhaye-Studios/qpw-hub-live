# Check the bus — LDI quickstart

Read `0000-INDEX.md` first. It is a current directory; history is a separate reference.
Use artifact IDs, full checksums and base commits from the current index or handoff.

## Receive

```sh
python bus.py brief --state /private/codex/brief.sqlite --budget-chars 6000
```

The brief shows new/changed task cards within a **character** budget. Originals are
preserved. `deferred` means more cached messages remain; `Next server page` means
more may exist beyond this fetch. It is never a declaration that all work is done.

```sh
# Read a required original; use --refresh for the current canonical row.
python bus.py brief-show --state /private/codex/brief.sqlite MESSAGE_ID --refresh
# Continue deferred cached cards without another network fetch.
python bus.py brief --state /private/codex/brief.sqlite --cache
# Continue the server's keyset page when a next cursor is printed.
python bus.py brief --state /private/codex/brief.sqlite --after CURSOR
```

Read originals marked `Details=required` before acting. Verify referenced specs and
artifact hashes. Evidence labels are the **sender's claims**, not independent proof.
Do already-authorized work; return a deliverable or the exact missing input. Gate
labels route work; they never authorize a merge, publication, spending or account change.

After reading and durably retaining the task in your work state:

```sh
python bus.py brief-ack --state /private/codex/brief.sqlite VIEW_RECEIPT
```

This suppresses an unchanged **view**, not the task. It sends no BUS message and
does not change canonical message status. Defer acknowledgment until an original
you need has been read and retained. `--all` reviews seen work again; refreshed
status or content changes reappear. Cached views are explicitly labeled as cached.

## Send one clear work packet

```sh
python bus.py compose --sender luna --recipient codex --kind task \
  --topic "WO16: file verified payload" --task-id WO16 \
  --do "Verify the staged ZIP and file one PR against main." \
  --deliver "PR URL and hash evidence on BUS." \
  --constraint "Do not merge; preserve canonical event bytes." \
  --evidence REPORTED --requires-reply --gate NEEDS_RON_MERGE \
  --summary "The verified payload is in the artifact references." \
  --artifact-refs /private/refs.json --outbox /private/spool/outgoing
```

Use `task`, `result`, `blocker`, `decision` or `info`:

- **Do**: one concrete receiver action.
- **Deliver**: the output or verified result, preferably a reference.
- **Blocked by**: one exact missing file/tool/permission, not a vague delay.
- **Constraints**: short, explicit guardrails. Never bury them in a preview.
- **Evidence**: OBSERVED / REPORTED / INFERRED / UNKNOWN.
- **Reply needed**: true only when a substantive response is required.

Keep source facts/specs in referenced artifacts. Full IDs and checksums print once
in the brief's artifact table. Retry the same envelope file/ID; do not recompose it.
Status receipts use the acknowledgment path, not “received / agreed / thanks” BUS
messages. No changes means no new BUS post. Never silently merge separate tasks
just because their topics or prose look similar.

Private `BUS_NODE` delegation and the existing authd surrogate are unchanged.
Node RPC migration, cloud Drive integration and runtime rollout remain separate
review steps in README.md. This is not a claim that an idle chat session can poll
or act without a running integration.

— CODEX
