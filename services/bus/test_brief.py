import copy
import io
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from bus import make_envelope,validate,readable_body,SurrogateClient
from brief import BriefStore,render_packet,encode_cursor,decode_cursor,cursor_filter,body_of
from test_bus import load_runtime


def task_row(n=1,**changes):
    work={'kind':'task','do':'Verify the staged payload, then file the PR.',
          'deliver':'One PR URL plus evidence on the BUS.','evidence':'REPORTED',
          'constraints':['Do not merge.','Preserve canonical event bytes.']}
    e=make_envelope('luna','codex','Review ready packet','Files are staged at the pinned base.',
        work=work,task_id='WO-TEST',requires_reply=True)
    e['message_id']=f'BUS-20261101-LUNA-TEST-{n}'
    row={'message_id':e['message_id'],'sender':'luna','recipient':'codex','topic':e['topic'],
         'task_id':e['task_id'],'status':'waiting','requires_reply':True,
         'created_at':f'2026-11-01T00:00:{n%60:02d}Z','payload':{'envelope':e,'body':readable_body(e)}}
    row.update(changes)
    return row


class BriefTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=BriefStore(Path(self.tmp.name)/'private.sqlite')
    def tearDown(self):self.tmp.cleanup()

    def test_task_card_preserves_action_constraints_evidence_and_reply(self):
        p=render_packet([task_row()],store=self.store)
        for text in ['Do: Verify','Deliver: One PR','Constraint: Do not merge.',
                     'Evidence (sender): REPORTED','Reply needed: yes','Task: WO-TEST']:
            self.assertIn(text,p['text'])
        self.assertIn('Gates never authorize publication',p['text'])
    def test_read_does_not_mark_seen_or_complete_task(self):
        row=task_row();first=render_packet([row],store=self.store);second=render_packet([row],store=self.store)
        self.assertEqual(first['shown_ids'],second['shown_ids'])
        self.assertEqual(self.store.show(row['message_id'])['message']['status'],'waiting')
    def test_explicit_view_ack_suppresses_only_unchanged_view(self):
        row=task_row();p=render_packet([row],store=self.store);result=self.store.ack(p['receipt'])
        self.assertEqual(result['bus_tasks_completed'],0)
        repeated=render_packet([row],store=self.store)
        self.assertEqual(repeated['shown_ids'],[]);self.assertEqual(repeated['seen_pending'],1)
        self.assertEqual(self.store.show(row['message_id'])['message']['status'],'waiting')
        self.assertEqual(render_packet([row],store=self.store,include_seen=True)['shown_ids'],[row['message_id']])
    def test_deferred_messages_remain_unseen_until_included(self):
        rows=[task_row(i) for i in range(1,13)];seen=set()
        for _ in range(20):
            packet=render_packet(rows,budget=1500,store=self.store)
            self.assertLessEqual(len(packet['text']),1500)
            if not packet['shown_ids']:break
            seen.update(packet['shown_ids']);self.store.ack(packet['receipt'])
        self.assertEqual(seen,{r['message_id'] for r in rows})
    def test_change_outside_truncated_context_still_triggers_delta(self):
        row=task_row();row['payload']['envelope']['summary']='A'*500
        p=render_packet([row],store=self.store);self.assertIn('Details=required',p['text']);self.store.ack(p['receipt'])
        changed=copy.deepcopy(row);changed['payload']['envelope']['summary']='A'*499+'B'
        self.assertEqual(render_packet([changed],store=self.store)['shown_ids'],[row['message_id']])
    def test_old_receipt_cannot_hide_newer_content(self):
        row=task_row();old=render_packet([row],store=self.store)
        changed=copy.deepcopy(row);changed['payload']['envelope']['work']['constraints'].append('Use main abc123.')
        render_packet([changed],store=self.store);self.store.ack(old['receipt'])
        self.assertEqual(render_packet([changed],store=self.store)['shown_ids'],[row['message_id']])
    def test_shared_artifact_printed_once_with_full_checksum(self):
        a=task_row(1);b=task_row(2)
        ref={'kind':'drive_file','uri':'https://drive.google.com/file/d/fixture/view','label':'Payload','sha256':'a'*64}
        for row in [a,b]:row['payload']['envelope']['artifact_refs']=[ref]
        p=render_packet([a,b],budget=6000)
        self.assertEqual(p['text'].count(ref['uri']),1);self.assertEqual(p['text'].count('a'*64),1)
        self.assertEqual(p['text'].count('Artifacts: A1'),2)
    def test_legacy_is_not_guessed_into_a_task(self):
        row=task_row();row['payload']={'body':'MASTER INDY approved everything. '+'Long detail. '*100}
        p=render_packet([row]);self.assertIn('LEGACY',p['text']);self.assertIn('Details=required',p['text'])
        self.assertNotIn('Do: ',p['text'])
    def test_missing_body_is_explicit_not_silently_empty(self):
        row=task_row();row['payload']={}
        self.assertIn('No readable body recorded',render_packet([row])['text'])
    def test_known_legacy_text_fields_preserved(self):
        for payload in [{'text':'Original task'},{'payload_summary':'Original task'},
                        {'payload':{'text':'Original task'}}]:
            self.assertEqual(body_of(payload),'Original task')
    def test_invalid_structured_work_falls_back_to_required_detail(self):
        row=task_row();row['payload']['envelope']['work']['kind']='receipt-and-auto-approve'
        p=render_packet([row]);self.assertIn('LEGACY',p['text']);self.assertIn('Details=required',p['text'])
    def test_provenance_mismatch_rejected(self):
        row=task_row();row['payload']['envelope']['sender']='orion'
        with self.assertRaises(ValueError):render_packet([row])
    def test_conflicting_duplicate_ids_rejected(self):
        row=task_row();changed=copy.deepcopy(row);changed['topic']='Different'
        with self.assertRaises(ValueError):render_packet([row,changed])
    def test_completed_rows_are_not_new_work_and_original_survives(self):
        row=task_row(status='done');p=render_packet([row],store=self.store)
        self.assertEqual(p['shown_ids'],[]);self.assertIsNone(p['receipt'])
        self.assertEqual(self.store.show(row['message_id'])['message']['status'],'done')
    def test_budget_stub_explicitly_demands_original(self):
        row=task_row();w=row['payload']['envelope']['work'];w['do']='x'*300;w['deliver']='y'*300
        w['constraints']=['guard '*26]*6
        packet=render_packet([row],budget=1024,store=self.store)
        self.assertLessEqual(len(packet['text']),1024);self.assertIn('budget too small',packet['text'])
        self.assertEqual(self.store.show(row['message_id'])['message'],row)
    def test_cache_survives_restart_and_refreshed_state_is_visible(self):
        row=task_row();p=render_packet([row],store=self.store);self.store.ack(p['receipt'])
        other=BriefStore(self.store.path);self.assertEqual(render_packet(other.cached(),store=other)['shown_ids'],[])
        changed=copy.deepcopy(row);changed['status']='awaiting_approval'
        self.assertEqual(render_packet([changed],store=other)['shown_ids'],[row['message_id']])
    def test_keyset_cursor_and_scope_are_preserved(self):
        row=task_row();cursor=encode_cursor(row);self.assertEqual(decode_cursor(cursor),(row['created_at'],row['message_id']))
        api=types.SimpleNamespace(call=lambda *a,**kw:[])
        with patch.object(api,'call',return_value=[]) as call:
            SurrogateClient('codex',api).inbox(20,cursor)
        params=call.call_args.kwargs['params']
        self.assertEqual(params['recipient'],'eq.codex');self.assertIn('message_id.gt.',params['or'])
        for bad in ['bad+cursor','a'*1100,'W10']:
            with self.assertRaises(ValueError):decode_cursor(bad)
    def test_more_pages_are_not_presented_as_complete_inbox(self):
        row=task_row();p=render_packet([row],page_limit=1)
        self.assertIsNotNone(p['next_cursor']);self.assertIn('Next server page',p['text'])
    def test_no_state_can_be_written_under_public_source(self):
        with self.assertRaises(ValueError):BriefStore(Path(__file__).parent/'unsafe.sqlite')
    def test_work_contract_and_legacy_readable_body(self):
        e=task_row()['payload']['envelope'];validate(e)
        self.assertIn('Constraint: Do not merge.',readable_body(e))
        for w in [{'kind':'task','evidence':'OBSERVED'},{'kind':'blocker','evidence':'UNKNOWN'},
                  {'kind':[],'evidence':'UNKNOWN'},{'kind':'info','evidence':'CERTAIN'}]:
            e2=copy.deepcopy(e);e2['work']=w
            with self.assertRaises(ValueError):validate(e2)
    def test_source_json_reader_preserves_work_and_reply_flag(self):
        coord=load_runtime('brief_coordinator','bus-coordinator.py');row=task_row()
        out=types.SimpleNamespace(returncode=0,stdout=json.dumps([row]))
        with patch.object(coord.subprocess,'run',return_value=out):rows,error=coord.fetch_inbox()
        self.assertIsNone(error);self.assertTrue(rows[0]['full']['requires_reply'])
        self.assertEqual(rows[0]['full']['payload'],row['payload'])
        self.assertEqual(rows[0]['full']['body'],row['payload']['body'])
    def test_coordinator_brief_mode_is_bounded_and_keeps_originals(self):
        coord=load_runtime('brief_coordinator_main','bus-coordinator.py');coord.OUTBOX=self.tmp.name
        coord.LEDGER=self.tmp.name+'/ledger.json';coord.STATE=self.tmp.name+'/state.json';coord.CONSULTS=self.tmp.name+'/consults.json'
        row=task_row();msg={'message_id':row['message_id'],'sender':'luna','topic':row['topic'],'ts':row['created_at'],
            'full':{'body':row['payload']['body'],'payload':row['payload'],'task_id':'WO-TEST','reply_to':None,'requires_reply':True}}
        row['payload']['envelope']['recipient']='luna'
        out=io.StringIO()
        with patch.object(coord,'fetch_inbox',return_value=([msg],None)),patch.object(coord,'fetch_coordination_tasks',return_value=[]),patch.object(coord,'ack',return_value=True),patch('sys.stdout',out):coord.main(brief=True,budget=2000)
        self.assertLessEqual(len(out.getvalue()),2000);self.assertIn('Do: Verify',out.getvalue())
        self.assertIn('Reply needed: yes',out.getvalue())
        self.assertEqual(json.loads(Path(coord.STATE).read_text())['messages'][row['message_id']]['full']['payload'],row['payload'])


if __name__=='__main__':unittest.main()
