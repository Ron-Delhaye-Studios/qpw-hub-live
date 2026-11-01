"""Deterministic, bounded BUS triage views; never executes or acknowledges BUS tasks.

Full records remain in a private local cache and canonical BUS. A view receipt is
explicitly accepted by the consumer; only displayed records become 'seen'.
"""
import base64
import hashlib
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path


def compact(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def digest(value):return hashlib.sha256(compact(value).encode()).hexdigest()
def one_line(value):return ' '.join(str(value or '').split())
def shorten(value,limit):
    value=one_line(value)
    return (value if len(value)<=limit else value[:limit-1]+'…',len(value)>limit)


def body_of(payload):
    """Known legacy text fields only; never guess an instruction from arbitrary JSON."""
    if not isinstance(payload,dict):return ''
    e=payload.get('envelope')
    if isinstance(e,dict) and isinstance(e.get('summary'),str):return e['summary']
    for key in ['body','text','payload_summary','summary']:
        if isinstance(payload.get(key),str) and payload[key].strip():return payload[key]
    inner=payload.get('payload')
    if isinstance(inner,dict):
        for key in ['body','text']:
            if isinstance(inner.get(key),str):return inner[key]
    return ''


def source_body_of(payload):
    """The coordinator scans the full human-readable body, not its context-only summary."""
    if isinstance(payload,dict) and isinstance(payload.get('body'),str) and payload['body'].strip():return payload['body']
    return body_of(payload)


def normalize(row):
    if not isinstance(row,dict) or not isinstance(row.get('message_id'),str) or not row['message_id']:
        raise ValueError('row requires stable message ID')
    p=row.get('payload') or {}
    if not isinstance(p,dict):p={}
    e=p.get('envelope') or {}
    if not isinstance(e,dict):e={}
    work=e.get('work')
    invalid_work=False
    if work is not None:
        from bus import validate
        try:validate(e)
        except (ValueError,TypeError):work=None;invalid_work=True
    for key in ['message_id','sender','recipient']:
        if key in e and e[key]!=row.get(key):raise ValueError('row/envelope provenance mismatch')
    if 'requires_reply' in e and e['requires_reply'] is not (row.get('requires_reply') is True):
        raise ValueError('row/envelope reply requirement mismatch')
    refs=e.get('artifact_refs',p.get('artifact_refs',[]))
    if not isinstance(refs,list):refs=[]
    return {'message_id':row['message_id'],'sender':row.get('sender','UNKNOWN'),
        'recipient':row.get('recipient','UNKNOWN'),'topic':row.get('topic',''),
        'task_id':row.get('task_id') or e.get('task_id'),
        'reply_to_message_id':row.get('reply_to_message_id') or e.get('reply_to_message_id'),
        'status':row.get('status','UNKNOWN'),'requires_reply':row.get('requires_reply') is True,
        'created_at':row.get('created_at',''),'summary':body_of(p),'work':work,
        'invalid_work':invalid_work,'gate':e.get('gate',p.get('gate','UNSPECIFIED')),
        'artifacts':[ref for ref in refs if isinstance(ref,dict)]}


def fingerprint(row):
    # Changes beyond a visible preview still trigger a fresh notice. Never hash the preview.
    return digest({k:row.get(k) for k in ['message_id','sender','recipient','topic','task_id',
        'reply_to_message_id','status','requires_reply','created_at','payload']})


def encode_cursor(row):
    timestamp=row.get('created_at');mid=row.get('message_id')
    if not timestamp or not mid:return None
    return base64.urlsafe_b64encode(compact([timestamp,mid]).encode()).decode().rstrip('=')


def decode_cursor(cursor):
    if not isinstance(cursor,str) or len(cursor)>1000 or not re.fullmatch('[A-Za-z0-9_-]+',cursor):
        raise ValueError('invalid inbox cursor')
    try:
        values=json.loads(base64.urlsafe_b64decode(cursor+'='*(-len(cursor)%4)))
        if not isinstance(values,list) or len(values)!=2:raise ValueError()
        timestamp,mid=values
        if not isinstance(timestamp,str) or not isinstance(mid,str) or len(mid)>300 or not mid:raise ValueError()
        if len(timestamp)>64 or datetime.fromisoformat(timestamp.replace('Z','+00:00')).tzinfo is None:raise ValueError()
    except (ValueError,TypeError,UnicodeError):raise ValueError('invalid inbox cursor') from None
    return timestamp,mid


def cursor_filter(cursor):
    timestamp,mid=decode_cursor(cursor)
    stamp=json.dumps(timestamp);message_id=json.dumps(mid)
    return f'(created_at.gt.{stamp},and(created_at.eq.{stamp},message_id.gt.{message_id}))'


class BriefStore:
    def __init__(self,path):
        self.path=Path(path)
        if self.path.is_symlink():raise ValueError('state path cannot be a symlink')
        code_dir=Path(__file__).resolve().parent
        static_root=code_dir.parent.parent if code_dir.name=='bus' and code_dir.parent.name=='services' else code_dir
        if self.path.resolve().is_relative_to(static_root):raise ValueError('private brief state must be outside the source/static repository')
        self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with sqlite3.connect(self.path) as db:
            db.execute('create table if not exists sources(mid text primary key,fingerprint text not null,seen text,row_json text not null)')
            db.execute('create table if not exists packets(receipt text primary key,shown_json text not null,acknowledged integer not null default 0)')
        os.chmod(self.path,0o600)

    def cache(self,rows):
        with sqlite3.connect(self.path) as db:
            for row in rows:
                mid=normalize(row)['message_id']
                db.execute('insert into sources(mid,fingerprint,row_json) values(?,?,?) on conflict(mid) do update set fingerprint=excluded.fingerprint,row_json=excluded.row_json',
                           (mid,fingerprint(row),compact(row)))

    def cached(self):
        with sqlite3.connect(self.path) as db:
            rows=[json.loads(r[0]) for r in db.execute('select row_json from sources')]
        return sorted(rows,key=lambda r:(str(r.get('created_at') or ''),r['message_id']))

    def seen(self,mid,fp):
        with sqlite3.connect(self.path) as db:
            row=db.execute('select seen from sources where mid=?',(mid,)).fetchone()
        return row is not None and row[0]==fp

    def prepare(self,receipt,rows):
        # Save full originals before returning a view. No task or BUS receipt is modified.
        with sqlite3.connect(self.path) as db:
            db.execute('insert into packets(receipt,shown_json) values(?,?)',
                       (receipt,compact([[row['message_id'],fingerprint(row)] for row in rows])))

    def ack(self,receipt):
        with sqlite3.connect(self.path) as db:
            packet=db.execute('select shown_json,acknowledged from packets where receipt=?',(receipt,)).fetchone()
            if packet is None:raise ValueError('unknown local view receipt')
            if not packet[1]:
                for mid,fp in json.loads(packet[0]):db.execute('update sources set seen=? where mid=?',(fp,mid))
                db.execute('update packets set acknowledged=1 where receipt=?',(receipt,))
        return {'view_acknowledged':True,'bus_tasks_completed':0}

    def show(self,mid):
        with sqlite3.connect(self.path) as db:
            row=db.execute('select row_json from sources where mid=?',(mid,)).fetchone()
        if row is None:raise ValueError('message is not in this private cache; fetch the canonical inbox')
        return {'source':'local cached original; refresh inbox for live state','message':json.loads(row[0])}


def artifact_key(ref):
    return digest({k:ref.get(k) for k in ['kind','drive_id','uri','sha256','base_commit','version']})


def render_candidate(rows,receipt,total,seen_count,deferred,*,stub=False,next_cursor=None,source='input batch'):
    lines=[f'BUS BRIEF | new/changed {total} | shown {len(rows)} | deferred {deferred} | seen pending {seen_count}',
           'Source: '+source,
           'Read originals when Details=required. Evidence labels are sender claims. Gates never authorize publication.']
    artifacts={}
    for raw in rows:
        m=normalize(raw);w=m['work'];kind=w['kind'].upper() if w else 'LEGACY'
        if stub:
            lines.extend(['',f"{kind} | ID {m['message_id']}",'Details=required; fetch original; budget too small for its task card.'])
            continue
        lines.extend(['',f"{kind} | {one_line(m['sender'])} | {one_line(m['topic'])}",f"ID: {m['message_id']}",
                      f"Task: {one_line(m['task_id']) or 'UNSPECIFIED'} | State: {one_line(m['status'])} | Gate: {one_line(m['gate'])}"])
        if m['reply_to_message_id']:lines.append('Reply to: '+one_line(m['reply_to_message_id']))
        truncated=False
        if w:
            for key,label in [('do','Do'),('deliver','Deliver'),('blocked_by','Blocked by')]:
                if w.get(key):lines.append(label+': '+one_line(w[key]))
            for c in w.get('constraints',[]):lines.append('Constraint: '+one_line(c))
            lines.append('Evidence (sender): '+w['evidence'])
            preview,truncated=shorten(m['summary'],160)
            if preview:lines.append('Context: '+preview)
        else:
            preview,truncated=shorten(m['summary'],180)
            lines.append('Context: '+(preview or '[No readable body recorded; fetch original row.]'))
        refs=[]
        for ref in m['artifacts']:
            key=artifact_key(ref)
            if key not in artifacts:artifacts[key]=(f'A{len(artifacts)+1}',ref)
            refs.append(artifacts[key][0])
        if refs:lines.append('Artifacts: '+', '.join(dict.fromkeys(refs)))
        details=not w or truncated or m['invalid_work']
        lines.append('Reply needed: '+('yes' if m['requires_reply'] else 'no')+' | Details='+('required' if details else 'complete task card; verify referenced specs'))
    if artifacts:
        lines.extend(['','ARTIFACTS (packet-local labels; full IDs/checksums below)'])
        for alias,ref in artifacts.values():
            lines.append(alias+': '+compact(ref))
    if next_cursor:lines.append('\nNext server page: --after '+next_cursor)
    if receipt:lines.append('\nView receipt: '+receipt+' (ack only after reading/persisting work; does not complete BUS tasks)')
    return '\n'.join(lines)+'\n'


def render_packet(rows,*,budget=6000,store=None,include_seen=False,page_limit=None,source='input batch'):
    if not isinstance(budget,int) or not 1024<=budget<=100000:raise ValueError('budget must be 1024..100000 characters')
    # Preserve every ID. Duplicate IDs with different content in one read are an integrity error.
    unique={}
    for row in rows:
        mid=normalize(row)['message_id']
        if mid in unique and fingerprint(unique[mid])!=fingerprint(row):raise ValueError('conflicting duplicate message ID')
        unique[mid]=row
    ordered=sorted(unique.values(),key=lambda r:(str(r.get('created_at') or ''),r['message_id']))
    if store:store.cache(ordered)
    pending=[r for r in ordered if r.get('status') not in ('done','rejected')]
    changed=[r for r in pending if include_seen or not store or not store.seen(r['message_id'],fingerprint(r))]
    seen_count=len(pending)-len(changed)
    receipt=uuid.uuid4().hex if changed and store else None
    cursor=encode_cursor(ordered[-1]) if ordered and page_limit and len(rows)>=page_limit else None
    shown=[]
    for row in changed:
        candidate=render_candidate(shown+[row],receipt,len(changed),seen_count,len(changed)-len(shown)-1,next_cursor=cursor,source=source)
        if len(candidate)>budget:break
        shown.append(row)
    stub=False
    if changed and not shown:shown=[changed[0]];stub=True
    text=render_candidate(shown,receipt,len(changed),seen_count,len(changed)-len(shown),stub=stub,next_cursor=cursor,source=source)
    if len(text)>budget:raise ValueError('message metadata exceeds minimum packet budget; use a larger budget')
    if store and shown:store.prepare(receipt,shown)
    return {'text':text,'characters':len(text),'shown_ids':[r['message_id'] for r in shown],
            'deferred':len(changed)-len(shown),'seen_pending':seen_count,'receipt':receipt,'next_cursor':cursor}
