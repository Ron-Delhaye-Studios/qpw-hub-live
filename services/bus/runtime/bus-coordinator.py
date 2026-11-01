#!/usr/bin/env python3
"""Bus coordinator — the better system.

Every 10 minutes (via cron), this:
1. Polls LUNA's LDI bus inbox for new waiting messages.
2. Auto-acknowledges receipt on the bus (receipt only — never approval).
3. Scans each new message for AUTHORIZATION CLAIMS ("MASTER INDY approved/said/told...").
4. Verifies each claim against bus-authorizations.json — a ledger that only ever
   gains entries from MASTER INDY's own words. An LDI's claim NEVER writes to the ledger.
5. Marks per-message state: received -> acknowledged -> answered | blocked | awaiting_user.
6. Reports only genuinely new messages, with claim verdicts.

Output: JSON {"new":[{message_id,sender,topic,ts,claims:[{text,verdict,ledger_id}],state}], "error"?}
   plus "coordination_tasks":[{task_id,title,status,owner}] — open items from the
   shared coordination ledger (public.coordination_tasks), read-only, best-effort.
Verdicts: VERIFIED (ledger entry covers it) | UNVERIFIED (no ledger entry — held for MASTER INDY).
"""
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from brief import source_body_of,BriefStore,render_packet
import urllib.parse
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request, DynamicCredentialError

OUTBOX = os.getenv('QPW_BUS_STATE_DIR','/home/hatch/workspace/ldi-bus-outbox')
BUS_API = os.getenv('QPW_BUS_API',str(Path(__file__).with_name('bus_api.py')))
LEDGER = os.path.join(OUTBOX, 'bus-authorizations.json')
STATE = os.path.join(OUTBOX, 'bus-coordinator-state.json')
CONSULTS = os.path.join(OUTBOX, 'bus-consults.json')

SUPABASE_API = "https://ypfhcqeispdunewubxjm.supabase.co/rest/v1"
SUPABASE_ALLOWED = ("ypfhcqeispdunewubxjm.supabase.co",)
SUPABASE_CRED = "custom.supabase"

CLAIM_PATTERNS = [
    re.compile(r"(master indy|indy)\s+(said|approved|authorized|told(?: me| us)?|asked|requested|directed|confirmed)[^.]{0,160}", re.I),
    re.compile(r"per\s+(master indy|indy)[^.]{0,160}", re.I),
    re.compile(r"(with|under|on)\s+(his|master indy'?s|indy'?s)\s+(approval|authorization|authority|direction)[^.]{0,160}", re.I),
    re.compile(r"he\s+(approved|authorized|said|asked|requested)[^.]{0,160}", re.I),
    re.compile(r"at\s+(master indy|indy)'s\s+(explicit\s+|direct\s+)?request[^.]{0,200}", re.I),
    re.compile(r"(explicit|direct)\s+request\s+from\s+(master indy|indy)[^.]{0,200}", re.I),
    re.compile(r"(master indy|indy)\s+directive[^.]{0,160}", re.I),
]


def load_json(path, default):
    try:
        with open(path) as f:return json.load(f)
    except FileNotFoundError:
        return default


def save_json(path, obj):
    directory=os.path.dirname(path) or '.'
    os.makedirs(directory,exist_ok=True)
    name=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',dir=directory,delete=False) as f:
            name=f.name;json.dump(obj,f,indent=1);f.flush();os.fsync(f.fileno())
        os.replace(name,path)
        fd=os.open(directory,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:
        if name and os.path.exists(name):os.unlink(name)


def fetch_inbox():
    out = subprocess.run(
        [sys.executable, BUS_API, 'inbox', 'luna', '--status', 'waiting', '--limit', '20', '--json'],
        capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        return None, f"inbox rc={out.returncode} (request/credential details omitted)"
    try:
        rows=json.loads(out.stdout)
        if not isinstance(rows,list):raise ValueError('not rows')
        msgs=[{'message_id':r['message_id'],'sender':r['sender'],'topic':r['topic'],
               'ts':r['created_at'],'full':{'body':source_body_of(r.get('payload')),'payload':r.get('payload') or {},
                   'requires_reply':r.get('requires_reply') is True,
                   'reply_to':r.get('reply_to_message_id'),'task_id':r.get('task_id')}} for r in rows]
    except (ValueError,KeyError,TypeError):
        return None,'inbox returned invalid structured data; no messages acknowledged'
    return msgs, None


def fetch_full(mid):
    out = subprocess.run([sys.executable, BUS_API, 'get', mid],
                         capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        return {'body': '', 'reply_to': None, 'task_id': None}
    try:
        d = json.loads(out.stdout)
    except Exception:
        return {'body': '', 'reply_to': None, 'task_id': None}
    p = d.get('payload', {})
    return {'body': p.get('body', '') or '',
            'reply_to': d.get('reply_to_message_id'),
            'task_id': d.get('task_id')}


def find_claims(body):
    claims = []
    for pat in CLAIM_PATTERNS:
        for m in pat.finditer(body or ''):
            claims.append(m.group(0).strip())
    # dedupe, keep order
    seen, out = set(), []
    for c in claims:
        k = c.lower()
        if k not in seen:
            seen.add(k)
            out.append(c)
    return out


def verify_claim(claim, ledger):
    words = set(re.findall(r'[a-z]{4,}', claim.lower()))
    best, best_score = None, 0
    for e in ledger.get('entries', []):
        keys = set(e.get('scope_keywords', []))
        score = len(words & keys)
        if score > best_score:
            best, best_score = e, score
    if best and best_score >= 2:
        return 'VERIFIED', best['id']
    return 'UNVERIFIED', None


def ack(mid):
    try:
        out=subprocess.run([sys.executable, BUS_API, 'ack', mid, '--status', 'acknowledged'],
                       capture_output=True, text=True, timeout=30)
        if out.returncode!=0:return False
        result=json.loads(out.stdout)
        return result.get('acked')==1 and result.get('status')=='acknowledged'
    except Exception:
        return False


def reconcile_consults(new_full):
    consults = load_json(CONSULTS, {'consults': {}})
    now = datetime.now(timezone.utc)
    answered = 0
    for nm in new_full:
        rt, tid = nm.get('reply_to'), nm.get('task_id')
        for mid, c in consults['consults'].items():
            if c.get('status') != 'open':
                continue
            if (rt and rt == mid) or (tid and tid == c.get('task_id')):
                c['status'] = 'answered'
                c['answered_by'] = nm['message_id']
                c['answered_ts'] = now.isoformat()
                answered += 1
    overdue = []
    for mid, c in consults['consults'].items():
        if c.get('status') == 'open' and c.get('deadline_ts') and c['deadline_ts'] < now.isoformat():
            c['status'] = 'overdue'
            overdue.append({'message_id': mid, 'to': c.get('to'),
                            'task_id': c.get('task_id'), 'topic': c.get('topic'),
                            'deadline_ts': c.get('deadline_ts')})
    save_json(CONSULTS, consults)
    return answered, overdue


def fetch_coordination_tasks():
    """Read-only pull of open coordination tasks from the shared ledger.

    Returns a list of {task_id, title, status, owner}. On any failure
    returns [] so a ledger outage can never break the bus watch."""
    try:
        params = {
            "select": "task_id,title,status,owner,updated_at",
            "status": "in.(queued,in_progress,blocked,ready_for_review)",
            "order": "updated_at.desc",
        }
        url = SUPABASE_API + "/coordination_tasks?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, method="GET",
                                     headers={"Accept": "application/json"})
        add_surrogate_to_request(req, SUPABASE_CRED, allowed_hosts=SUPABASE_ALLOWED)
        resp = urllib.request.urlopen(req, timeout=30)
        rows = json.loads(resp.read().decode("utf-8") or "[]")
        return [{"task_id": r.get("task_id"), "title": r.get("title"),
                 "status": r.get("status"), "owner": r.get("owner")}
                for r in rows]
    except Exception:
        return []


def main(brief=False,budget=6000):
    ledger = load_json(LEDGER, {'entries': []})
    state = load_json(STATE, {'messages': {}})
    msgs, err = fetch_inbox()
    coord_tasks = fetch_coordination_tasks()
    if err:
        if brief:
            print('BUS unavailable; no receipt changed. '+err)
            return
        print(json.dumps({'new': [], 'overdue': [], 'coordination_tasks': coord_tasks,
                          'error': err}))
        return
    seen_ids = set(state['messages'].keys())
    new_out = []
    new_full = []
    for m in msgs:
        mid = m['message_id']
        if mid in seen_ids:
            if not state['messages'][mid].get('acked'):
                state['messages'][mid]['acked']=ack(mid)
                if state['messages'][mid]['acked']:
                    state['messages'][mid]['status']=state['messages'][mid].get('target_status','acknowledged')
                save_json(STATE,state)
            continue
        full = m['full']
        body = full['body']
        new_full.append({'message_id': mid, 'reply_to': full['reply_to'],
                         'task_id': full['task_id']})
        claims_raw = find_claims(body)
        claims = []
        for c in claims_raw:
            verdict, lid = verify_claim(c, ledger)
            claims.append({'text': c[:220], 'verdict': verdict, 'ledger_id': lid})
        st = 'awaiting_user' if any(c['verdict'] == 'UNVERIFIED' for c in claims) else 'acknowledged'
        state['messages'][mid] = {
            'sender': m['sender'], 'topic': m['topic'], 'ts': m['ts'],
            'acked': False, 'claims': claims, 'status': 'received','target_status':st,'full':full,
        }
        # Persist the entire task before acknowledging transport receipt.
        save_json(STATE,state)
        state['messages'][mid]['acked']=ack(mid)
        if state['messages'][mid]['acked']:state['messages'][mid]['status']=st
        save_json(STATE,state)
        new_out.append({'message_id': mid, 'sender': m['sender'], 'topic': m['topic'],
                        'ts': m['ts'], 'claims': claims, 'state': state['messages'][mid]['status'],
                        'body_preview': body[:600]})
    _, overdue = reconcile_consults(new_full)
    # Never discard unresolved tasks just because more than 200 have arrived.
    # Retention/archiving needs an explicit policy; messages remain recoverable here.
    save_json(STATE, state)
    pending=[{'message_id':mid,'sender':m.get('sender'),'topic':m.get('topic'),'status':m.get('status')}
        for mid,m in state['messages'].items() if m.get('status') not in ('answered','done')]
    if brief:
        rows=[]
        for mid,m in state['messages'].items():
            if m.get('status') in ('answered','done'):continue
            full=m.get('full') or {}
            rows.append({'message_id':mid,'sender':m.get('sender'),'recipient':'luna',
                'topic':m.get('topic'),'created_at':m.get('ts',''),'status':m.get('status'),
                'task_id':full.get('task_id'),'reply_to_message_id':full.get('reply_to'),
                'requires_reply':full.get('requires_reply',False),
                'payload':full.get('payload',{'body':full.get('body','')})})
        note=f'Coordination ledger: {len(coord_tasks)} rows returned separately; use full output for that source.\n'
        packet=render_packet(rows,budget=budget-len(note),store=BriefStore(os.path.join(OUTBOX,'bus-brief.sqlite')),
            source='private coordinator state; transport receipt is not task completion')
        print(note+packet['text'],end='')
        return
    print(json.dumps({'new': new_out, 'overdue': overdue,
                      'coordination_tasks': coord_tasks,
                      'pending':pending[:50],'pending_count':len(pending),
                      'pending_truncated':len(pending)>50}))


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--brief',action='store_true');parser.add_argument('--budget-chars',type=int,default=6000)
    args=parser.parse_args()
    main(brief=args.brief,budget=args.budget_chars)

