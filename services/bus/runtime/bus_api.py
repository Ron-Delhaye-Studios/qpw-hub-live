#!/usr/bin/env python3
"""LDI message bus CLI over the Supabase Data API (PostgREST).

The bus is one table: public.bus_messages. RLS is enabled with no policies,
so only the service_role credential (this bridge) can read or write it.
Nobody can UPDATE or DELETE through policy design: the bridge never issues
either; history is append-only by convention, enforced by code review.

Usage:
  bus_api.py send --from <ldi> --to <ldi|master-indy> --topic <topic> --body <text>
                  [--task-id ID] [--reply-to MID] [--requires-reply] [--message-id MID]
  bus_api.py inbox <ldi> [--status waiting] [--limit 20]
  bus_api.py ack <message_id> [--status acknowledged]
  bus_api.py get <message_id>

Never prints or logs the credential; authd substitutes the surrogate.
"""
import argparse
import json
import sys
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from bus import validate,read_envelope

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/templates/skill/bin")
from dynamic_credentials import (
    add_surrogate_to_request,
    read_json_response,
    DynamicCredentialError,
)

ALLOWED = ("ypfhcqeispdunewubxjm.supabase.co",)
CRED = "custom.supabase"
API = "https://ypfhcqeispdunewubxjm.supabase.co/rest/v1"
TABLE = "bus_messages"


def call(method, path, params=None, payload=None, prefer=None):
    url = API + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = None
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    if prefer is not None:
        headers['Prefer'] = prefer
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    if payload is not None:
        req.data = json.dumps(payload).encode("utf-8")
    add_surrogate_to_request(req, CRED, allowed_hosts=ALLOWED)
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        print(f"Supabase API error {e.code} (response body omitted)", file=sys.stderr)
        sys.exit(2)
    if resp.status in (204,):
        return {}
    raw = resp.read().decode("utf-8")
    if not raw.strip():
        return {}
    return json.loads(raw)


def cmd_send(args):
    payload = {
        "sender": args.frm,
        "recipient": args.to,
        "topic": args.topic,
        "task_id": args.task_id,
        "reply_to_message_id": args.reply_to,
        "status": "waiting",
        "requires_reply": args.requires_reply,
        "payload": {"body": args.body},
    }
    if args.message_id:
        payload["message_id"] = args.message_id
    rows = call("POST", f"/{TABLE}", payload=payload)
    row = rows[0] if isinstance(rows, list) and rows else rows
    print(json.dumps({"message_id": row.get("message_id"), "id": str(row.get("id"))}))


def cmd_inbox(args):
    params = {
        "recipient": f"eq.{args.ldi}",
        "order": "created_at.asc",
        "limit": str(args.limit),
        "select": "message_id,sender,recipient,topic,task_id,reply_to_message_id,"
                  "status,requires_reply,created_at,payload",
    }
    if args.status:
        params["status"] = f"eq.{args.status}"
    rows = call("GET", f"/{TABLE}", params=params)
    if args.json:
        print(json.dumps(rows))
        return
    for r in rows:
        body = (r.get("payload") or {}).get("body", "")
        print(f"[{r['created_at']}] {r['message_id']} {r['sender']} -> {r['recipient']} "
              f"| {r['topic']} | {r['status']}")
        if body:
            print(f"    {body[:300]}")
    print(f"-- {len(rows)} message(s) --", file=sys.stderr)


def cmd_ack(args):
    params = {"message_id": f"eq.{args.message_id}"}
    rows = call("PATCH", f"/{TABLE}", params=params,
                payload={"status": args.status})
    print(json.dumps({"acked": len(rows) if isinstance(rows, list) else 0,
                      "status": args.status}))


def cmd_get(args):
    params = {"message_id": f"eq.{args.message_id}", "limit": "1"}
    rows = call("GET", f"/{TABLE}", params=params)
    if not rows:
        print(json.dumps({"found": False}))
        return
    print(json.dumps(rows[0], indent=2, default=str))


def send_envelope(e):
    """Trusted LUNA service-role adapter, retaining authd credential handling unchanged."""
    validate(e)
    payload={'message_id':e['message_id'],'sender':e['sender'],'recipient':e['recipient'],
             'topic':e['topic'],'task_id':e.get('task_id'),'reply_to_message_id':e.get('reply_to_message_id'),
             'status':'waiting','requires_reply':e['requires_reply'],
             'payload':{'body':e['summary']+'\n\n— '+e['sender'].upper(),'via':'bus-v2','envelope':e}}
    rows=call('POST','/'+TABLE,params={'on_conflict':'message_id'},payload=payload,
              prefer='resolution=ignore-duplicates,return=representation')
    if rows:
        return {'message_id':e['message_id'],'duplicate':False}
    prior=call('GET','/'+TABLE,params={'message_id':'eq.'+e['message_id'],'limit':'1'})
    if not prior or prior[0]['sender']!=e['sender'] or prior[0]['payload'].get('envelope')!=e:
        raise ValueError('immutable message ID collision')
    return {'message_id':e['message_id'],'duplicate':True}


def main():
    ap = argparse.ArgumentParser(description="LDI message bus over Supabase")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("send")
    s.add_argument("--from", dest="frm", required=True)
    s.add_argument("--to", required=True)
    s.add_argument("--topic", required=True)
    s.add_argument("--body", required=True)
    s.add_argument("--task-id", default=None)
    s.add_argument("--reply-to", default=None)
    s.add_argument("--requires-reply", action="store_true")
    s.add_argument("--message-id", default=None)

    i = sub.add_parser("inbox")
    i.add_argument("ldi")
    i.add_argument("--status", default="waiting")
    i.add_argument("--limit", type=int, default=20)
    i.add_argument('--json',action='store_true',help='Structured full rows; no body truncation')

    e=sub.add_parser('send-envelope')
    e.add_argument('file')

    a = sub.add_parser("ack")
    a.add_argument("message_id")
    a.add_argument("--status", default="acknowledged",
                   choices=["waiting", "delivered", "acknowledged", "done",
                            "awaiting_approval"])

    g = sub.add_parser("get")
    g.add_argument("message_id")

    args = ap.parse_args()
    try:
        if args.cmd=='send-envelope':
            print(json.dumps(send_envelope(read_envelope(args.file))))
        else:
            {"send": cmd_send, "inbox": cmd_inbox, "ack": cmd_ack,
             "get": cmd_get}[args.cmd](args)
    except DynamicCredentialError as e:
        print(f"credential error: {e}", file=sys.stderr)
        sys.exit(3)


if __name__ == "__main__":
    main()

