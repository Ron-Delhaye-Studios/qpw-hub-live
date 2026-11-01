// Real PostgreSQL engine (PGlite/WASM); no live database or secrets are used.
import { PGlite } from '@electric-sql/pglite';
import fs from 'node:fs/promises';
import assert from 'node:assert/strict';

const db = new PGlite();
let checks = 0;
function ok(value, label) { assert.ok(value, label); checks++; }
async function rejected(fn, label) {
  let failed = false;
  try { await fn(); } catch { failed = true; }
  ok(failed, label);
}
await db.exec(`
  create role anon; create role authenticated; create role service_role;
  create table public.bus_messages(
    id uuid primary key default gen_random_uuid(),created_at timestamptz not null default now(),
    message_id text unique not null,sender text not null,recipient text not null,topic text not null,
    task_id text,reply_to_message_id text,status text not null default 'waiting',
    requires_reply boolean not null default false,payload jsonb not null default '{}');
  alter table public.bus_messages enable row level security;
  create function public._ldi_who(token text) returns text language sql as $$
    select case token when 'fixture-luna' then 'luna' when 'fixture-thaleia' then 'thaleia' end;
  $$;
`);
await db.exec(await fs.readFile(new URL('./sql/001_bus_v2.sql', import.meta.url), 'utf8'));

const envelope = (id, overrides={}) => ({schema_version:'0.3',message_id:`BUS-20261101-TEST-${id}`,
  sender:'luna',recipient:'thaleia',topic:'Fixture delivery',summary:'A substantive fixture finding.',
  created_at:'2026-11-01T00:00:00Z',requires_reply:true,gate:'AUTO_PEER',visibility:'private',
  artifact_refs:[],...overrides});
const send = (e, token='fixture-luna') => db.query('select public.ldi_send_v2($1,$2::jsonb) as value',[token,JSON.stringify(e)]);
const inbox = async (token='fixture-thaleia', limit=20) =>
  (await db.query('select public.ldi_inbox_v2($1,$2) as value',[token,limit])).rows[0].value;
const ack = (id,state,token='fixture-thaleia') =>
  db.query('select public.ldi_ack_v2($1,$2,$3)',[token,id,state]);

for (let i=0;i<25;i++) await send(envelope(String(i).padStart(2,'0')));
const first = await inbox();
ok(first.length===20,'bounded read');
ok(first[0].message_id.endsWith('-00'),'stable oldest first');
ok((await db.query("select count(*)::int n from public.bus_messages where status='waiting'")).rows[0].n===25,
   'read does not acknowledge returned or unreturned messages');
ok((await inbox())[0].message_id===first[0].message_id,'read replay until explicit processing');
await ack(first[0].message_id,'delivered');
ok((await inbox()).length===20,'delivery is not task completion');
await ack(first[0].message_id,'processed');
await ack(first[0].message_id,'delivered');
ok(!(await inbox()).some(x=>x.message_id===first[0].message_id),'receipt never downgrades');
ok((await db.query("select count(*)::int n from public.bus_messages where status='waiting'")).rows[0].n===24,
   'only explicitly acknowledged row changes');
ok((await send(envelope('01'))).rows[0].value.duplicate===true,'exact send retry deduplicates');
ok((await db.query('select count(*)::int n from public.bus_messages')).rows[0].n===25,'no duplicate row');
await rejected(()=>send(envelope('01',{summary:'Changed content'})),'immutable collision rejected');
await rejected(()=>send(envelope('bad-token'),'invalid'),'bad token rejected');
await rejected(()=>send(envelope('spoof',{sender:'thaleia'})),'sender spoof rejected');
await rejected(()=>ack(first[1].message_id,'processed','fixture-luna'),'wrong recipient cannot acknowledge');
for (const limit of [null,0,-1,101]) await rejected(()=>inbox('fixture-thaleia',limit),'invalid batch limit');
await send(envelope('ALL',{recipient:'all'}));
await ack('BUS-20261101-TEST-ALL','processed');
ok(!(await inbox('fixture-thaleia',100)).some(x=>x.message_id.endsWith('-ALL')),'broadcast receipt specific to thaleia');
ok((await inbox('fixture-luna',100)).some(x=>x.message_id.endsWith('-ALL')),'broadcast remains for luna');
ok((await db.query("select status from public.bus_messages where message_id='BUS-20261101-TEST-ALL'")).rows[0].status==='waiting',
   'broadcast global status unchanged');
for (const patch of [{requires_reply:'true'},{gate:'AUTO_PUBLISH'},{visibility:'unknown'},
  {summary:''},{created_at:'2026-99-99T00:00:00Z'},{extra:'unrecognized'},
  {artifact_refs:[{kind:'drive_file',uri:'https://drive.google.com/file/d/example/view',label:'missing hash'}]},
  {artifact_refs:[{kind:'repo_snapshot',uri:'https://drive.google.com/example',label:'snapshot',sha256:'a'.repeat(64)}]}]) {
  await rejected(()=>send(envelope('INVALID',patch)),'invalid contract rejected');
}
await send(envelope('ARTIFACT',{artifact_refs:[{kind:'repo_snapshot',uri:'https://drive.google.com/file/d/example/view',
  label:'pinned main',sha256:'a'.repeat(64),base_commit:'b'.repeat(40)}]}));
checks++;
await db.exec('set role anon');
await rejected(()=>db.query('select * from public.bus_receipts_v2'),'no direct anonymous receipt access');
await rejected(()=>db.query('select * from public.bus_messages'),'no direct anonymous message access');
ok((await inbox('fixture-luna',100)).length>0,'token-scoped RPC works with anonymous transport role');
await rejected(()=>inbox('bad-token',20),'anonymous role still needs valid node token');
await db.exec('reset role');
await db.close();
console.log(`PASS: ${checks} PostgreSQL contract/security/retry/receipt checks`);
