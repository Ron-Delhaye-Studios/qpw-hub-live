import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from bus import (make_envelope,validate,encode,atomic_write,read_envelope,sync_once,
                 TransportError,RPC,SurrogateClient)
from snapshot import build,verify


class FakeBus:
    node='thaleia'
    def __init__(self):self.rows=[];self.sent={};self.acks=[];self.attempts=0;self.timeout_once=False;self.failure=False
    def send(self,e):
        self.attempts+=1
        if self.failure:raise TransportError(False)
        self.sent.setdefault(e['message_id'],e)
        if self.timeout_once:self.timeout_once=False;raise TransportError(True)
        return {'message_id':e['message_id']}
    def inbox(self):return self.rows
    def ack(self,mid,state):self.acks.append((mid,state))


def row(e):return {'message_id':e['message_id'],'sender':e['sender'],'recipient':e['recipient'],'payload':{'envelope':e}}


class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)/'spool'
        self.bus=FakeBus()
    def tearDown(self):self.tmp.cleanup()
    def e(self,**kwargs):return make_envelope('thaleia','luna','A finding','Observed a concrete difference.',**kwargs)
    def outgoing(self,e):
        dest=self.root/'outgoing'/(e['message_id']+'.json');atomic_write(dest,e);return dest

    def test_compact_envelope_and_private_default(self):
        e=self.e();self.assertEqual(e['visibility'],'private');self.assertEqual(validate(e),e)
        self.assertNotEqual(e['message_id'],self.e()['message_id'])
    def test_gate_never_implicitly_approves(self):
        e=self.e(gate='NEEDS_RON_MERGE');self.assertEqual(e['gate'],'NEEDS_RON_MERGE')
        e['gate']='AUTO_DEPLOY'
        with self.assertRaises(ValueError):validate(e)
    def test_strict_boolean_identity_summary_and_artifact(self):
        for change in [{'requires_reply':'true'},{'sender':'THALEIA'},{'summary':''},
                       {'artifact_refs':[{'kind':'drive_file','uri':'https://drive.google.com/example','label':'x'}]}]:
            e=self.e();e.update(change)
            with self.assertRaises(ValueError):validate(e)
    def test_atomic_immutable_file_and_partial_ignored(self):
        e=self.e();p=self.outgoing(e);atomic_write(p,e)
        self.assertEqual(read_envelope(p),e)
        changed=dict(e,summary='Different')
        with self.assertRaises(ValueError):atomic_write(p,changed)
        (p.parent/'bad.partial').write_text('{')
        self.assertEqual(sync_once(self.bus,self.root,True)['imported'],1)
    def test_duplicate_json_keys_and_wrong_filename_rejected(self):
        e=self.e();p=self.outgoing(e)
        p.write_text(p.read_text().replace('"topic":','"sender":"luna","topic":'))
        with self.assertRaises(ValueError):read_envelope(p)
        p.write_bytes(encode(e));renamed=p.with_name('wrong.json');p.rename(renamed)
        with self.assertRaises(ValueError):read_envelope(renamed)
    def test_import_off_unless_explicitly_enabled(self):
        self.outgoing(self.e());self.assertEqual(sync_once(self.bus,self.root)['imported'],0)
        self.assertEqual(self.bus.attempts,0)
    def test_retry_after_ambiguous_send_keeps_same_id(self):
        e=self.e();self.outgoing(e);self.bus.timeout_once=True
        self.assertEqual(sync_once(self.bus,self.root,True)['retry'],1)
        self.assertEqual(sync_once(self.bus,self.root,True)['imported'],1)
        self.assertEqual(sync_once(self.bus,self.root,True)['imported'],0)
        self.assertEqual(len(self.bus.sent),1);self.assertEqual(self.bus.attempts,2)
    def test_unavailable_rpc_is_transport_not_dead_task(self):
        p=self.outgoing(self.e());self.bus.failure=True
        self.assertEqual(sync_once(self.bus,self.root,True)['retry'],1)
        self.assertTrue(p.exists());self.assertEqual(list((self.root/'dead-letter').iterdir()),[])
    def test_bad_envelope_preserved_in_dead_letter_once(self):
        p=self.outgoing(self.e());p.write_text('{}')
        self.assertEqual(sync_once(self.bus,self.root,True)['dead_letter'],1)
        self.assertEqual(sync_once(self.bus,self.root,True)['dead_letter'],0)
        self.assertTrue(p.exists());self.assertEqual(len(list((self.root/'dead-letter').iterdir())),1)
    def test_other_node_cannot_be_imported_as_local_node(self):
        e=make_envelope('luna','thaleia','Finding','Body');self.outgoing(e)
        self.assertEqual(sync_once(self.bus,self.root,True)['dead_letter'],1)
        self.assertEqual(self.bus.attempts,0)
    def test_only_explicitly_shared_rows_export_and_never_mark_done(self):
        shared=make_envelope('luna','thaleia','Finding','Body',visibility='shared')
        private=make_envelope('luna','thaleia','Private','Body')
        self.bus.rows=[row(shared),row(private),{'payload':{'body':'legacy'}}]
        result=sync_once(self.bus,self.root)
        self.assertEqual(result['exported'],1);self.assertEqual(result['deferred_private_or_legacy'],2)
        self.assertEqual(self.bus.acks,[(shared['message_id'],'delivered')])
        self.assertEqual(read_envelope(self.root/'incoming'/(shared['message_id']+'.json')),shared)
    def test_row_identity_mismatch_fails_closed(self):
        e=make_envelope('luna','thaleia','Finding','Body',visibility='shared');r=row(e);r['sender']='orion';self.bus.rows=[r]
        with self.assertRaises(ValueError):sync_once(self.bus,self.root)
        self.assertEqual(self.bus.acks,[])
    def test_node_rpc_rejects_legacy_fallback(self):
        c=RPC('https://example.invalid','fixture-public-key','fixture-node-token','thaleia')
        with self.assertRaises(ValueError):c.call('ldi_send',{})
        with self.assertRaises(ValueError):RPC.from_env()


def load_runtime(name,file):
    fake=types.ModuleType('dynamic_credentials')
    fake.add_surrogate_to_request=lambda *a,**kw:None
    fake.read_json_response=lambda *a:None
    fake.DynamicCredentialError=type('DynamicCredentialError',(Exception,),{})
    with patch.dict(sys.modules,{'dynamic_credentials':fake}):
        spec=importlib.util.spec_from_file_location(name,Path(__file__).parent/'runtime'/file)
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    return mod


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.api=load_runtime('api_fixture','bus_api.py')
        self.coord=load_runtime('coordinator_fixture','bus-coordinator.py')
    def test_json_inbox_preserves_pipe_topic_and_full_body_in_one_call(self):
        r={'message_id':'one','sender':'thaleia','recipient':'luna','topic':'A | B','created_at':'2026-11-01',
           'payload':{'body':'x'*2000},'task_id':'task','reply_to_message_id':'parent'}
        out=types.SimpleNamespace(returncode=0,stdout=json.dumps([r]),stderr='')
        with patch.object(self.coord.subprocess,'run',return_value=out) as run:
            rows,error=self.coord.fetch_inbox()
        self.assertIsNone(error);self.assertEqual(rows[0]['topic'],'A | B')
        self.assertEqual(rows[0]['full']['body'],'x'*2000);self.assertEqual(run.call_count,1)
        self.assertIn('--json',run.call_args.args[0])
    def test_failed_ack_command_or_zero_rows_is_not_success(self):
        for rc,body in [(2,''),(0,'{"acked":0,"status":"acknowledged"}')]:
            with patch.object(self.coord.subprocess,'run',return_value=types.SimpleNamespace(returncode=rc,stdout=body)):
                self.assertFalse(self.coord.ack('mid'))
    def test_json_inbox_invalid_data_not_silently_empty(self):
        with patch.object(self.coord.subprocess,'run',return_value=types.SimpleNamespace(returncode=0,stdout='not-json')):
            rows,error=self.coord.fetch_inbox()
        self.assertIsNone(rows);self.assertIn('invalid structured',error)
    def test_atomic_state_failure_preserves_previous_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'state.json';p.write_text('{"old":true}')
            with patch.object(self.coord.os,'replace',side_effect=OSError('fixture')):
                with self.assertRaises(OSError):self.coord.save_json(str(p),{'new':True})
            self.assertEqual(json.loads(p.read_text()),{'old':True})
    def test_full_task_persisted_before_ack_and_pending_not_trimmed(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=self.coord;c.LEDGER=tmp+'/ledger.json';c.STATE=tmp+'/state.json';c.CONSULTS=tmp+'/consults.json'
            existing={f'old-{i}':{'status':'acknowledged','acked':True,'topic':'Old'} for i in range(201)}
            c.save_json(c.STATE,{'messages':existing})
            msg={'message_id':'new-one','sender':'thaleia','topic':'Task','ts':'2026-11-01',
                 'full':{'body':'complete payload','reply_to':None,'task_id':'task'}}
            def ack(mid):
                saved=json.loads(Path(c.STATE).read_text())
                self.assertEqual(saved['messages'][mid]['full']['body'],'complete payload')
                return False
            out=io.StringIO()
            with patch.object(c,'fetch_inbox',return_value=([msg],None)),patch.object(c,'fetch_coordination_tasks',return_value=[]),patch.object(c,'ack',side_effect=ack),patch('sys.stdout',out):c.main()
            saved=json.loads(Path(c.STATE).read_text());report=json.loads(out.getvalue())
            self.assertEqual(len(saved['messages']),202);self.assertFalse(saved['messages']['new-one']['acked'])
            self.assertEqual(report['pending_count'],202);self.assertTrue(report['pending_truncated'])
            self.assertEqual(len(report['pending']),50)
    def test_surrogate_send_replay_is_immutable_and_preserves_auth_pattern(self):
        e=make_envelope('luna','thaleia','Finding','Body')
        with patch.object(self.api,'call',side_effect=[[],[{'sender':'luna','payload':{'envelope':e}}]]) as call:
            self.assertTrue(self.api.send_envelope(e)['duplicate'])
            self.assertIn('ignore-duplicates',call.call_args_list[0].kwargs['prefer'])
        with patch.object(self.api,'call',side_effect=[[],[{'sender':'luna','payload':{'envelope':dict(e,summary='Changed')}}]]):
            with self.assertRaises(ValueError):self.api.send_envelope(e)
        self.assertEqual(self.api.CRED,'custom.supabase')
        self.assertEqual(self.api.ALLOWED,('ypfhcqeispdunewubxjm.supabase.co',))


class SnapshotTests(unittest.TestCase):
    def test_complete_pinned_reproducible_archive_excludes_untracked(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=Path(tmp)/'repo';r.mkdir()
            def git(*a):return subprocess.check_output(['git','-C',str(r),*a],stderr=subprocess.DEVNULL)
            git('init');git('config','user.email','fixture@example.invalid');git('config','user.name','Fixture')
            (r/'off-season').mkdir();(r/'off-season/off-season.json').write_text('{"windows":[]}')
            (r/'index.html').write_text('Fixture');git('add','.');git('commit','-m','Fixture')
            (r/'.env').write_text('untracked fixture secret')
            a=build(r,'HEAD',Path(tmp)/'one.zip');b=build(r,'HEAD',Path(tmp)/'two.zip')
            self.assertEqual(a['sha256'],b['sha256']);self.assertEqual(a['files'],2)
            self.assertEqual(verify(a['file'])['base_commit'],git('rev-parse','HEAD').decode().strip())


if __name__=='__main__':unittest.main()
