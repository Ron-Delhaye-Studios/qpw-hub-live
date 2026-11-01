"""QPW BUS v0.3 candidate: compact envelopes, idempotent RPCs, atomic local drop.

No raw SQL, message execution, legacy-Doc parsing, Drive permission changes or publishing.
The local directory adapter is a handoff point for LUNA's authenticated Drive watcher.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LIMIT = 32768
REQUIRED = {'schema_version','message_id','sender','recipient','topic','summary','created_at',
            'requires_reply','gate','visibility','artifact_refs'}
OPTIONAL = {'task_id','reply_to_message_id'}
ARTIFACT_KEYS = {'kind','uri','label','sha256','base_commit','version','drive_id'}


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def validate(e):
    if not isinstance(e, dict) or not REQUIRED <= e.keys() or not e.keys() <= REQUIRED | OPTIONAL:
        raise ValueError('envelope keys do not match v0.3')
    if len(encode(e)) > LIMIT:
        raise ValueError('envelope too large; use artifact references')
    for k in REQUIRED - {'requires_reply','artifact_refs'}:
        if not isinstance(e[k], str):
            raise ValueError('invalid field type: ' + k)
    if e['schema_version'] != '0.3' or not re.fullmatch(r'BUS-[0-9]{8}-[A-Z0-9_-]{1,150}', e['message_id']):
        raise ValueError('invalid schema version or message ID')
    for k in ['sender','recipient']:
        if not re.fullmatch(r'[a-z][a-z0-9-]{0,39}', e[k]):
            raise ValueError('node names must be canonical lowercase')
    if not 1 <= len(e['topic'].strip()) <= 200 or not 1 <= len(e['summary'].strip()) <= 2000:
        raise ValueError('invalid topic/summary length')
    if type(e['requires_reply']) is not bool:
        raise ValueError('requires_reply must be boolean')
    if e['gate'] not in {'AUTO_PEER','NEEDS_RON_MERGE','NEEDS_RON_DECISION'}:
        raise ValueError('invalid gate; gate metadata never grants permission')
    if e['visibility'] not in {'private','shared'}:
        raise ValueError('invalid visibility')
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z', e['created_at']):
        raise ValueError('created_at must be UTC ISO8601')
    datetime.fromisoformat(e['created_at'].replace('Z','+00:00'))
    for k in OPTIONAL:
        if e.get(k) is not None and (not isinstance(e[k], str) or not 1 <= len(e[k]) <= 200):
            raise ValueError('invalid threading field')
    if not isinstance(e['artifact_refs'], list) or len(e['artifact_refs']) > 10:
        raise ValueError('invalid artifact list')
    for ref in e['artifact_refs']:
        if not isinstance(ref, dict) or not {'kind','uri','label'} <= ref.keys() or not ref.keys() <= ARTIFACT_KEYS:
            raise ValueError('invalid artifact keys')
        if any(not isinstance(v,str) or len(v)>2048 for v in ref.values()):
            raise ValueError('invalid artifact metadata')
        if ref['kind'] not in {'drive_file','drive_doc','github_pr','github_commit','repo_snapshot'}:
            raise ValueError('invalid artifact kind')
        if not re.fullmatch(r'https://[^\s]+', ref['uri']) or not 1 <= len(ref['label'].strip()) <= 200:
            raise ValueError('invalid artifact URL/label')
        if ref['kind'] in {'drive_file','repo_snapshot'} and 'sha256' not in ref:
            raise ValueError('file artifact needs checksum')
        if ref['kind']=='repo_snapshot' and 'base_commit' not in ref:
            raise ValueError('snapshot needs base commit')
        if 'sha256' in ref and not re.fullmatch(r'[0-9a-f]{64}',ref['sha256']):
            raise ValueError('bad artifact checksum')
        if 'base_commit' in ref and not re.fullmatch(r'[0-9a-f]{40}',ref['base_commit']):
            raise ValueError('bad base commit')
    return e


def make_envelope(sender, recipient, topic, summary, *, gate='AUTO_PEER', visibility='private',
                  artifact_refs=None, task_id=None, reply_to_message_id=None, requires_reply=False):
    now = datetime.now(timezone.utc)
    e = dict(schema_version='0.3', message_id=f'BUS-{now:%Y%m%d}-{sender.upper()}-{uuid.uuid4().hex.upper()}',
             sender=sender, recipient=recipient, topic=topic, summary=summary,
             created_at=now.isoformat(timespec='seconds').replace('+00:00','Z'),
             requires_reply=requires_reply, gate=gate, visibility=visibility, artifact_refs=artifact_refs or [])
    for k,v in [('task_id',task_id),('reply_to_message_id',reply_to_message_id)]:
        if v is not None:
            e[k]=v
    return validate(e)


def unique_keys(pairs):
    out = {}
    for k,v in pairs:
        if k in out:
            raise ValueError('duplicate JSON key')
        out[k]=v
    return out


def read_envelope(path):
    p=Path(path)
    if p.is_symlink() or not p.is_file() or p.stat().st_size>LIMIT:
        raise ValueError('not a bounded ordinary envelope file')
    e=validate(json.loads(p.read_bytes(),object_pairs_hook=unique_keys))
    if p.name != e['message_id']+'.json':
        raise ValueError('filename must match immutable message ID')
    return e


def atomic_write(path, e):
    """Create once without overwrite; final filename is never partially written."""
    validate(e)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    data=encode(e)
    tmp=None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent,prefix='.',suffix='.partial',delete=False) as f:
            tmp=Path(f.name);f.write(data);f.flush();os.fsync(f.fileno())
        try:
            os.link(tmp,path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes()!=data:
                raise ValueError('immutable file collision')
        fd=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:
        if tmp is not None:tmp.unlink(missing_ok=True)


class TransportError(Exception):
    def __init__(self, retryable):
        self.retryable=retryable
        super().__init__('BUS transport failed; request content and credentials omitted')


class RPC:
    def __init__(self, url, apikey, token, node):
        if not url.startswith('https://') or not re.fullmatch(r'[a-z][a-z0-9-]{0,39}',node):
            raise ValueError('invalid HTTPS endpoint or node')
        self.url,self.apikey,self.token,self.node=url.rstrip('/'),apikey,token,node

    @classmethod
    def from_env(cls):
        raise ValueError('Node RPC requires explicit existing-runtime injection; no credentials are loaded here')

    def call(self, name, args):
        if name not in {'ldi_send_v2','ldi_inbox_v2','ldi_ack_v2'}:
            raise ValueError('legacy/non-idempotent RPC fallback is disabled')
        data=encode({'p_token':self.token,**args})
        for attempt in range(3):
            request=Request(self.url+'/rest/v1/rpc/'+name,data,
                {'apikey':self.apikey,'Content-Type':'application/json'},method='POST')
            try:
                with urlopen(request,timeout=20) as response:return json.load(response)
            except HTTPError as error:
                retryable=error.code>=500 or error.code in (408,429)
            except (URLError,TimeoutError,OSError,ValueError):
                retryable=True
            if not retryable or attempt==2:raise TransportError(retryable)
            time.sleep(2**attempt)

    def send(self,e):
        validate(e)
        if e['sender']!=self.node:raise ValueError('local sender differs from configured node')
        result=self.call('ldi_send_v2',{'p_envelope':e})
        if not isinstance(result,dict) or result.get('message_id')!=e['message_id']:
            raise TransportError(True)
        return result

    def inbox(self,limit=20):
        result=self.call('ldi_inbox_v2',{'p_limit':limit})
        if not isinstance(result,list):raise TransportError(True)
        return result

    def ack(self,mid,state):
        if state not in {'delivered','processed'}:raise ValueError('invalid receipt')
        return self.call('ldi_ack_v2',{'p_message_id':mid,'p_state':state})


class SurrogateClient:
    """Private LUNA runtime only; reuses the shared source's authd path verbatim."""
    def __init__(self,node,api):
        if not re.fullmatch(r'[a-z][a-z0-9-]{0,39}',node):raise ValueError('invalid node')
        self.node,self.api=node,api

    @classmethod
    def from_env(cls):
        node=os.getenv('BUS_NODE')
        if not node:raise ValueError('Set BUS_NODE to the explicitly delegated spool identity')
        path=Path(__file__).parent/'runtime'/'bus_api.py'
        spec=importlib.util.spec_from_file_location('qpw_surrogate_api',path)
        api=importlib.util.module_from_spec(spec);spec.loader.exec_module(api)
        return cls(node,api)

    def send(self,e):
        if e['sender']!=self.node:raise ValueError('sender mismatch')
        try:return self.api.send_envelope(e)
        except SystemExit:raise TransportError(False) from None

    def inbox(self,limit=20):
        if not 1<=limit<=100:raise ValueError('invalid limit')
        try:
            return self.api.call('GET','/bus_messages',params={'recipient':'eq.'+self.node,
                'status':'eq.waiting','order':'created_at.asc,message_id.asc','limit':str(limit),
                'select':'message_id,sender,recipient,topic,task_id,reply_to_message_id,status,requires_reply,created_at,payload'})
        except SystemExit:raise TransportError(False) from None

    def ack(self,mid,state):
        if state not in {'delivered','processed'}:raise ValueError('invalid receipt')
        try:
            rows=self.api.call('PATCH','/bus_messages',params={'message_id':'eq.'+mid,
                'recipient':'eq.'+self.node,'status':'not.in.(done,rejected)'},
                payload={'status':'acknowledged' if state=='delivered' else 'done'})
            return {'message_id':mid,'receipt':state,'changed':len(rows)}
        except SystemExit:raise TransportError(False) from None


def sync_once(rpc, root, allow_import=False):
    """Local spool adapter only. It never reads/writes a shared Drive Doc or executes tasks."""
    root=Path(root);root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if root.is_symlink() or root.stat().st_mode & 0o022:
        raise ValueError('spool must be private and not group/world writable')
    incoming=root/'incoming';outgoing=root/'outgoing';quarantine=root/'dead-letter'
    for d in [incoming,outgoing,quarantine]:d.mkdir(exist_ok=True,mode=0o700)
    journal=root/'transport.sqlite'
    if any(d.is_symlink() for d in [incoming,outgoing,quarantine,journal]):
        raise ValueError('spool paths must not be symlinks')
    result={'imported':0,'exported':0,'deferred_private_or_legacy':0,'dead_letter':0,'retry':0}
    with sqlite3.connect(journal) as db:
        db.execute('create table if not exists files(name text primary key,digest text,state text)')
        os.chmod(journal,0o600)
        if allow_import:
            for p in sorted(outgoing.glob('*.json')):
                if p.is_symlink() or p.stat().st_size>LIMIT:raise ValueError('unsafe outgoing file')
                digest=hashlib.sha256(p.read_bytes()).hexdigest()
                previous=db.execute('select digest,state from files where name=?',(p.name,)).fetchone()
                if previous and previous[0]==digest and previous[1] in ('sent','dead'):continue
                try:
                    if previous and previous[0]!=digest:raise ValueError('immutable outgoing file changed')
                    e=read_envelope(p)
                    if e['sender']!=rpc.node:raise ValueError('sender must match configured node')
                    rpc.send(e) # Stable message ID survives crash after send, before journal commit.
                    state='sent';result['imported']+=1
                except TransportError:
                    # An unavailable/uninstalled/misconfigured RPC is transport failure,
                    # not an invalid task. Keep the source pending even for a terminal HTTP error.
                    result['retry']+=1;continue
                except ValueError:
                    # Preserve source, do not delete/archive another node's message.
                    dest=quarantine/(p.name+'.'+digest)
                    if not dest.exists():
                        with dest.open('xb') as f:f.write(p.read_bytes())
                    state='dead';result['dead_letter']+=1
                db.execute('insert into files values(?,?,?) on conflict(name) do update set digest=excluded.digest,state=excluded.state',
                           (p.name,digest,state));db.commit()
        for row in rpc.inbox():
            e=row.get('payload',{}).get('envelope')
            if not e or e.get('visibility')!='shared':
                result['deferred_private_or_legacy']+=1;continue
            validate(e)
            if e['recipient'] not in (rpc.node,'all') or row.get('recipient')!=e['recipient'] or row.get('sender')!=e['sender'] or row.get('message_id')!=e['message_id']:
                raise ValueError('row/envelope provenance mismatch')
            dest=incoming/(e['message_id']+'.json')
            existed=dest.exists()
            atomic_write(dest,e)
            rpc.ack(e['message_id'],'delivered') # durable LOCAL spool receipt, never a task completion.
            if not existed:result['exported']+=1
    return result


def main():
    os.umask(0o077)
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    compose=sub.add_parser('compose')
    for name in ['sender','recipient','topic','summary']:compose.add_argument('--'+name,required=True)
    compose.add_argument('--outbox',required=True)
    compose.add_argument('--gate',default='AUTO_PEER');compose.add_argument('--shared',action='store_true')
    compose.add_argument('--artifact-refs',help='Path to a small JSON artifact-reference array')
    compose.add_argument('--task-id');compose.add_argument('--reply-to');compose.add_argument('--requires-reply',action='store_true')
    check=sub.add_parser('check');check.add_argument('file')
    send=sub.add_parser('send');send.add_argument('file')
    inbox=sub.add_parser('inbox');inbox.add_argument('--limit',type=int,default=20)
    ack=sub.add_parser('ack');ack.add_argument('message_id');ack.add_argument('state',choices=['delivered','processed'])
    sync=sub.add_parser('sync');sync.add_argument('--spool',required=True);sync.add_argument('--allow-import',action='store_true')
    verify=sub.add_parser('verify');verify.add_argument('file');verify.add_argument('sha256')
    a=p.parse_args()
    if a.command=='compose':
        refs=None
        if a.artifact_refs:
            data=Path(a.artifact_refs).read_bytes()
            if len(data)>LIMIT:raise ValueError('artifact-reference file too large')
            refs=json.loads(data,object_pairs_hook=unique_keys)
        e=make_envelope(a.sender,a.recipient,a.topic,a.summary,gate=a.gate,
            visibility='shared' if a.shared else 'private',task_id=a.task_id,
            reply_to_message_id=a.reply_to,requires_reply=a.requires_reply,artifact_refs=refs)
        dest=Path(a.outbox)/(e['message_id']+'.json');atomic_write(dest,e);print(dest)
    elif a.command=='check':
        e=read_envelope(a.file);print(json.dumps({'message_id':e['message_id'],'valid':True}))
    elif a.command=='verify':
        if not re.fullmatch('[0-9a-f]{64}',a.sha256):raise ValueError('invalid SHA256')
        if hashlib.sha256(Path(a.file).read_bytes()).hexdigest()!=a.sha256:raise ValueError('artifact hash mismatch')
        print('SHA256 MATCH')
    else:
        rpc=SurrogateClient.from_env()
        if a.command=='send':print(json.dumps(rpc.send(read_envelope(a.file))))
        elif a.command=='inbox':print(json.dumps(rpc.inbox(a.limit)))
        elif a.command=='ack':print(json.dumps(rpc.ack(a.message_id,a.state)))
        else:print(json.dumps(sync_once(rpc,a.spool,a.allow_import)))


if __name__=='__main__':main()
