import hashlib
import hmac
import http.client
import json
import os
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from http.server import ThreadingHTTPServer
from unittest.mock import patch

from bridge import Settings, Store, Worker, RemoteError, make_handler, signature_valid, Clients


class FakeClients:
    def __init__(self):
        self.generations = []
        self.deliveries = []
        self.fail_generate = None
        self.fail_send = None

    def generate(self, history, body):
        self.generations.append((history, body))
        if self.fail_generate:
            raise self.fail_generate
        return 'CODEX: ' + body

    def send(self, sender, reply, mid):
        self.deliveries.append((sender, reply, mid))
        if self.fail_send:
            raise self.fail_send
        return 'wamid.reply'


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.s = Settings('test-secret', 'test-verify', 'test-meta', '12345', 'v23.0',
                          'test-openai', 'configured-test-model', frozenset({'15555550123'}),
                          self.temp.name + '/private.sqlite')
        self.store = Store(self.s.db_path)
        self.clients = FakeClients()
        self.worker = Worker(self.store, self.clients)

    def tearDown(self):
        self.temp.cleanup()

    def payload(self, mid='wamid.inbound', sender='15555550123', body='Hello CODEX', timestamp=None, phone='12345'):
        return {'object': 'whatsapp_business_account', 'entry': [{'changes': [{'field': 'messages', 'value': {
            'metadata': {'phone_number_id': phone}, 'messages': [{'from': sender, 'id': mid,
            'timestamp': str(int(time.time()) if timestamp is None else timestamp),
            'type': 'text', 'text': {'body': body}}]}}]}]}

    def row(self, mid='wamid.inbound'):
        with self.store.connect() as db:
            return db.execute('SELECT * FROM messages WHERE id=?', (mid,)).fetchone()

    def test_signature_exact_raw_bytes(self):
        raw = b'{"message":"hello"}'
        sig = 'sha256=' + hmac.new(b'test-secret', raw, hashlib.sha256).hexdigest()
        self.assertTrue(signature_valid(raw, sig, self.s.app_secret))
        for bad in [None, '', sig[:-1], 'sha256=\u2603', sig.upper()]:
            self.assertFalse(signature_valid(raw, bad, self.s.app_secret))
        self.assertFalse(signature_valid(raw + b' ', sig, self.s.app_secret))

    def test_duplicate_delivery_queues_once(self):
        self.assertEqual(self.store.ingest(self.payload(), self.s), 1)
        self.assertEqual(self.store.ingest(self.payload(), self.s), 0)
        self.assertTrue(self.worker.step())
        self.assertFalse(self.worker.step())
        self.assertEqual(len(self.clients.deliveries), 1)
        self.assertEqual(self.row()['state'], 'sent')

    def test_closed_allowlist_phone_and_time_window(self):
        for payload in [self.payload(sender='15555550999'), self.payload(phone='999'),
                        self.payload(timestamp=time.time()-86400), self.payload(timestamp=time.time()+600)]:
            self.assertEqual(self.store.ingest(payload, self.s), 0)
        self.assertFalse(self.worker.step())

    def test_status_only_and_nontext_do_not_trigger_model(self):
        payload = self.payload()
        value = payload['entry'][0]['changes'][0]['value']
        value['messages'][0]['type'] = 'image'
        self.assertEqual(self.store.ingest(payload, self.s), 0)
        value.pop('messages')
        value['statuses'] = [{'status': 'delivered'}]
        self.assertEqual(self.store.ingest(payload, self.s), 0)

    def test_conversation_survives_restart_and_stays_sender_scoped(self):
        self.store.ingest(self.payload(body='First'), self.s)
        self.worker.step()
        store = Store(self.s.db_path)
        store.ingest(self.payload(mid='second', body='Remember that?'), self.s)
        Worker(store, self.clients).step()
        self.assertEqual(self.clients.generations[1][0], [
            {'role': 'user', 'content': 'First'}, {'role': 'assistant', 'content': 'CODEX: First'}])
        self.assertEqual(store.history('15555550999', 999), [])

    def test_timeout_after_send_never_auto_resends(self):
        self.clients.fail_send = RemoteError()
        self.store.ingest(self.payload(), self.s)
        self.worker.step()
        self.assertEqual(self.row()['state'], 'uncertain')
        self.store.recover()
        self.assertFalse(self.worker.step())
        self.assertEqual(len(self.clients.deliveries), 1)

    def test_restart_recovers_generation_but_holds_inflight_send(self):
        self.store.ingest(self.payload(), self.s)
        self.store.ingest(self.payload(mid='second'), self.s)
        self.store.update('wamid.inbound', state='generating')
        self.store.update('second', state='sending', reply='Saved reply')
        self.store.recover()
        self.assertEqual(self.row()['state'], 'pending')
        self.assertEqual(self.row('second')['state'], 'uncertain')

    def test_generation_retry_keeps_chat_order(self):
        self.clients.fail_generate = RemoteError(429)
        self.store.ingest(self.payload(), self.s)
        self.store.ingest(self.payload(mid='second'), self.s)
        self.worker.step()
        self.assertEqual(self.row()['state'], 'pending')
        self.assertIsNone(self.store.next())
        self.assertEqual(self.clients.deliveries, [])
        self.clients.fail_generate = None
        self.store.update('wamid.inbound', next_attempt=0)
        self.worker.step()
        self.worker.step()
        self.assertEqual([x[2] for x in self.clients.deliveries], ['wamid.inbound', 'second'])

    def test_generation_auth_failure_is_terminal(self):
        self.clients.fail_generate = RemoteError(401)
        self.store.ingest(self.payload(), self.s)
        self.worker.step()
        self.assertEqual(self.row()['state'], 'failed')
        self.assertFalse(self.worker.step())

    def test_generated_reply_reused_after_restart(self):
        self.store.ingest(self.payload(), self.s)
        self.store.update('wamid.inbound', state='generated', reply='Saved reply')
        self.store.recover()
        self.worker.step()
        self.assertEqual(self.clients.generations, [])
        self.assertEqual(self.clients.deliveries[0][1], 'Saved reply')

    def test_expired_queue_item_is_not_sent(self):
        self.store.ingest(self.payload(), self.s)
        with patch('bridge.time.time', return_value=time.time()+86400):
            self.worker.step()
        self.assertEqual(self.row()['state'], 'expired')
        self.assertEqual(self.clients.deliveries, [])

    def test_meta_rejection_is_recorded(self):
        self.clients.fail_send = RemoteError(403)
        self.store.ingest(self.payload(), self.s)
        self.worker.step()
        self.assertEqual(self.row()['state'], 'rejected')

    def test_clients_use_official_endpoints_and_no_model_tools(self):
        with patch('bridge.post_json', return_value={'output': [{'type': 'message', 'role': 'assistant',
                   'content': [{'type': 'output_text', 'text': 'Hello'}]}]}) as post:
            self.assertEqual(Clients(self.s).generate([], 'Question'), 'Hello')
            args = post.call_args.args
            self.assertEqual(args[0], 'https://api.openai.com/v1/responses')
            self.assertFalse(args[2]['store'])
            self.assertNotIn('tools', args[2])
        with patch('bridge.post_json', return_value={'messages': [{'id': 'reply'}]}) as post:
            Clients(self.s).send('15555550123', 'Hello', 'original')
            self.assertEqual(post.call_args.args[0], 'https://graph.facebook.com/v23.0/12345/messages')
            self.assertEqual(post.call_args.args[2]['context'], {'message_id': 'original'})

    def test_missing_config_fails_closed(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, 'Missing configuration names'):
                Settings.from_env()

    def env(self):
        return {'META_APP_SECRET': 'test-secret', 'META_VERIFY_TOKEN': 'test-verify',
                'META_ACCESS_TOKEN': 'test-meta', 'META_PHONE_NUMBER_ID': '12345',
                'META_GRAPH_VERSION': 'v23.0', 'OPENAI_API_KEY': 'test-openai',
                'OPENAI_MODEL': 'configured-test-model', 'WHATSAPP_ALLOWED_SENDERS': '15555550123',
                'BRIDGE_DB': self.s.db_path}

    def test_config_accepts_deployed_layout_and_private_volume(self):
        with patch.dict(os.environ, self.env(), clear=True), patch('bridge.__file__', '/opt/codex-whatsapp/bridge.py'):
            self.assertEqual(Settings.from_env().db_path, self.s.db_path)

    def test_config_rejects_database_inside_static_repository(self):
        from pathlib import Path
        env = self.env()
        env['BRIDGE_DB'] = str(Path(__file__).resolve().parents[2] / 'unsafe.sqlite')
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, 'outside the repository'):
                Settings.from_env()

    def test_real_webhook_handshake_signature_and_durable_ack(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(self.s, self.store))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            conn = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=3)
            for token, status in [('wrong', 403), ('test-verify', 200)]:
                conn.request('GET', '/webhooks/whatsapp?hub.mode=subscribe&hub.challenge=123&hub.verify_token='+token)
                response = conn.getresponse()
                self.assertEqual(response.status, status)
                body = response.read()
                if status == 200:
                    self.assertEqual(body, b'123')
            raw = json.dumps(self.payload()).encode()
            conn.request('POST', '/webhooks/whatsapp', raw)
            response = conn.getresponse()
            self.assertEqual(response.status, 401)
            response.read()
            self.assertIsNone(self.row())
            sig = 'sha256=' + hmac.new(self.s.app_secret.encode(), raw, hashlib.sha256).hexdigest()
            conn.request('POST', '/webhooks/whatsapp', raw, {'X-Hub-Signature-256': sig})
            response = conn.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            self.assertEqual(self.row()['state'], 'pending')
            self.assertEqual(self.clients.generations, [])
            conn.close()
        finally:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    unittest.main()
