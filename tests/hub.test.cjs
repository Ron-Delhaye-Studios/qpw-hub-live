const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const board=require('../board.js');
const html=fs.readFileSync(require.resolve('../index.html'),'utf8');
function harness(fetcher,stored={}){
  const elements=new Map([...html.matchAll(/id="([^"]+)"/g)].map(m=>[m[1],{value:'',textContent:'',innerHTML:'',disabled:false,checked:true,classList:{add(){},remove(){}},focus(){}}]));
  const storage=seed=>{const m=new Map(Object.entries(seed));return {getItem:k=>m.get(k)||null,setItem:(k,v)=>m.set(k,v),removeItem:k=>m.delete(k)}};
  const intervals=[],events={};let now=Date.now();
  class Clock extends Date{static now(){return now}}
  const context={HubBoard:board,document:{getElementById:id=>elements.get(id),querySelectorAll:()=>[],addEventListener:(name,f)=>events[name]=f,hidden:false},localStorage:storage({}),sessionStorage:storage(stored),fetch:fetcher,AbortSignal,Date:Clock,crypto:require('node:crypto').webcrypto,setInterval:(f,ms)=>intervals.push({f,ms}),window:{scrollTo(){}}};
  vm.runInNewContext(html.match(/<script>\s*([\s\S]*?)<\/script>/)[1],context);
  return {el:id=>elements.get(id),context,intervals,advance:ms=>now+=ms};
}
const response=(data,status=200)=>({ok:status>=200&&status<300,status,json:async()=>data});
const state=()=>({fetched_at:new Date().toISOString(),tasks:[{task_id:'WO-5',title:'Build hub',owner:'codex',status:'in_progress',next_action:'Review'}],messages:[]});
const flush=()=>new Promise(resolve=>setImmediate(resolve));
test('Needs you uses explicit Indy ownership, not all blocked tasks or all approvals',()=>{
  const result=board.needs([{owner:'codex',status:'blocked'},{owner:'master-indy',status:'blocked'}],[{recipient:'luna',status:'awaiting_approval'},{recipient:'master-indy',status:'awaiting_approval'}]);
  assert.equal(result.length,2);assert.equal(result[0].owner,'master-indy');assert.equal(result[1].recipient,'master-indy');
});
test('structured content is preserved when supplied and missing projection is disclosed',()=>{
  assert.match(board.messageBody({payload:{result:'artifact@abc'}}),/artifact@abc/);
  assert.match(board.messageBody({body:null}),/structured content may be omitted/);
  assert.match(board.verification({status:'verified_done'}),/Verifier not recorded/);
});
test('key opens board without PIN setup, keeps token in session and uses protected no-store read',async()=>{
  const calls=[];const h=harness(async(url,opts)=>{calls.push({url,opts});return response(state())});
  h.el('access').value='test-private-key';h.el('keyform').onsubmit({preventDefault(){}});await flush();
  assert.equal(calls.length,1);assert.match(calls[0].url,/api=state/);assert.equal(calls[0].opts.headers['x-qpw-token'],'test-private-key');assert.equal(calls[0].opts.cache,'no-store');
  assert.equal(h.context.localStorage.getItem('qpw_token'),null);assert.equal(h.el('access').value,'');assert.match(h.el('orders').innerHTML,/WO-5/);
  assert.equal(h.el('connection').textContent,'Connected to BUS');
});
test('refresh failure retains last data and ages it as stale; hidden tabs do not poll',async()=>{
  let count=0;const h=harness(async()=>{if(++count>1)throw Error('offline');return response(state())},{qpw_token:'test'});await flush();
  const original=h.el('orders').innerHTML;await h.el('refresh').onclick();assert.equal(h.el('orders').innerHTML,original);assert.equal(h.el('connection').textContent,'STALE');
  h.context.document.hidden=true;h.intervals.find(i=>i.ms===30000).f();await flush();assert.equal(count,2);
});
test('old source timestamp and missing timestamp cannot appear fresh',async()=>{
  for(const ts of ['2000-01-01T00:00:00Z',undefined]){const d=state();d.fetched_at=ts;const h=harness(async()=>response(d),{qpw_token:'test'});await flush();assert.equal(h.el('connection').textContent,'STALE')}
});
test('age updates without a request and moves to stale after 60 seconds',async()=>{
  const h=harness(async()=>response(state()),{qpw_token:'test'});await flush();h.advance(61000);h.intervals.find(i=>i.ms===1000).f();assert.equal(h.el('connection').textContent,'STALE');
});
test('lock clears private data and late response cannot restore it',async()=>{
  let resolve;const h=harness(()=>new Promise(r=>resolve=r),{qpw_token:'test'});await flush();h.el('logout').onclick();resolve(response(state()));await flush();
  assert.equal(h.el('orders').textContent,'');assert.equal(h.el('connection').textContent,'Not connected');assert.equal(h.context.sessionStorage.getItem('qpw_token'),null);
});
test('unauthorized read clears session and invalid state never replaces valid data',async()=>{
  const h=harness(async()=>response({},401),{qpw_token:'test'});await flush();assert.equal(h.context.sessionStorage.getItem('qpw_token'),null);
  let count=0;const valid=harness(async()=>response(count++?{tasks:[null],messages:[]}:state()),{qpw_token:'test'});await flush();const original=valid.el('orders').innerHTML;await valid.el('refresh').onclick();assert.equal(valid.el('orders').innerHTML,original);assert.equal(valid.el('connection').textContent,'STALE');
});
test('ambiguous send retries same idempotency key; edited message gets a new key',async()=>{
  const sent=[];const h=harness(async(url,opts)=>{if(url.includes('api=send')){sent.push(JSON.parse(opts.body));throw Error('timeout')}return response(state())},{qpw_token:'test'});await flush();
  h.el('recipient').value='luna';h.el('topic').value='WO-5';h.el('body').value='Review';await h.el('send').onclick();await h.el('send').onclick();
  assert.equal(sent[0].idempotency_key,sent[1].idempotency_key);h.el('body').value='Updated review';await h.el('send').onclick();assert.notEqual(sent[1].idempotency_key,sent[2].idempotency_key);
});
test('message and task content are escaped before rendering',async()=>{
  const d=state();d.tasks[0].title='<img src=x onerror=alert(1)>';d.messages=[{message_id:'m',body:'<script>alert(1)</script>'}];const h=harness(async()=>response(d),{qpw_token:'test'});await flush();
  assert.match(h.el('orders').innerHTML,/&lt;img/);assert.match(h.el('messages').innerHTML,/&lt;script/);assert.doesNotMatch(h.el('orders').innerHTML,/<img/);
});
test('lock while send authorization is pending prevents the send request',async()=>{
  const calls=[];const h=harness(async(url)=>{calls.push(url);return response(state())},{qpw_token:'test'});await flush();
  h.el('recipient').value='luna';h.el('topic').value='Test';h.el('body').value='Test';const pending=h.el('send').onclick();h.el('logout').onclick();await pending;
  assert.equal(calls.filter(url=>url.includes('api=send')).length,0);assert.equal(h.el('sendstatus').textContent,'');
});
test('concurrent refresh requests share one read',async()=>{
  let count=0,resolve;const h=harness(()=>{count++;return new Promise(r=>resolve=r)},{qpw_token:'test'});await flush();
  const a=h.el('refresh').onclick(),b=h.el('refresh').onclick();assert.equal(count,1);resolve(response(state()));await Promise.all([a,b]);assert.equal(h.el('connection').textContent,'Connected to BUS');
});
