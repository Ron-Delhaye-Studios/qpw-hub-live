(function(root){
  'use strict';
  function isIndy(value){return String(value||'').trim().toLowerCase()==='master-indy'}
  function needs(tasks,messages){
    return tasks.filter(function(t){return isIndy(t.owner)&&['blocked','awaiting_approval','ready_for_review'].includes(t.status)})
      .concat(messages.filter(function(m){return isIndy(m.recipient)&&(m.status==='awaiting_approval'||(m.requires_reply&&['waiting','delivered'].includes(m.status)))}));
  }
  function messageBody(m){
    if(typeof m.body==='string'&&m.body.trim())return m.body;
    if(m.payload!=null)return typeof m.payload==='string'?m.payload:JSON.stringify(m.payload,null,2);
    return 'Content unavailable in this response. The current service returns only payload.body; structured content may be omitted.';
  }
  function verification(t){
    // Display the record without asserting that a receipt or an unversioned link proves completion.
    var e=t.evidence;
    return (t.verified_by?'Verifier: '+t.verified_by:'Verifier not recorded')+' · '+(t.verified_at?'Verification date: '+t.verified_at:'Verification date not recorded')+
      '\nEvidence (as recorded; check artifact reference and version): '+(e?typeof e==='string'?e:JSON.stringify(e,null,2):'not recorded');
  }
  var api={needs:needs,messageBody:messageBody,verification:verification};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.HubBoard=api;
})(typeof globalThis==='object'?globalThis:this);
