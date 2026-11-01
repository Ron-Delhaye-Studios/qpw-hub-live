-- REVIEW CANDIDATE. Additive only; do not apply automatically on website deploy.
-- Uses the existing per-node token verifier; does not mint or expose credentials.
begin;

create table public.bus_receipts_v2 (
  message_id text not null references public.bus_messages(message_id),
  recipient text not null,
  delivered_at timestamptz not null default now(),
  processed_at timestamptz,
  primary key (message_id, recipient)
);
alter table public.bus_receipts_v2 enable row level security;
revoke all on public.bus_receipts_v2 from public, anon, authenticated;

create function public.ldi_send_v2(p_token text, p_envelope jsonb)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare
  node text; k text; ref jsonb; prior public.bus_messages%rowtype; inserted_id text;
begin
  node := public._ldi_who(p_token);
  if node is null then raise exception 'bad token' using errcode='28000'; end if;
  if p_envelope is null or jsonb_typeof(p_envelope) <> 'object'
     or octet_length(p_envelope::text)>32768 then
    raise exception 'invalid envelope';
  end if;
  if not (p_envelope ?& array['schema_version','message_id','sender','recipient','topic',
      'summary','created_at','requires_reply','gate','visibility','artifact_refs']) then
    raise exception 'missing envelope fields';
  end if;
  for k in select jsonb_object_keys(p_envelope) loop
    if k <> all(array['schema_version','message_id','sender','recipient','topic','summary',
        'created_at','requires_reply','gate','visibility','artifact_refs','task_id','reply_to_message_id']) then
      raise exception 'unknown envelope field';
    end if;
  end loop;
  foreach k in array array['schema_version','message_id','sender','recipient','topic',
      'summary','created_at','gate','visibility'] loop
    if jsonb_typeof(p_envelope->k) is distinct from 'string' then
      raise exception 'invalid envelope field type';
    end if;
  end loop;
  if p_envelope->>'schema_version' <> '0.3'
      or (p_envelope->>'message_id') !~ '^BUS-[0-9]{8}-[A-Z0-9_-]{1,150}$'
      or (p_envelope->>'sender') <> node
      or (p_envelope->>'sender') !~ '^[a-z][a-z0-9-]{0,39}$'
      or (p_envelope->>'recipient') !~ '^[a-z][a-z0-9-]{0,39}$'
      or length(btrim(p_envelope->>'topic')) not between 1 and 200
      or length(btrim(p_envelope->>'summary')) not between 1 and 2000
      or (p_envelope->>'gate') not in ('AUTO_PEER','NEEDS_RON_MERGE','NEEDS_RON_DECISION')
      or (p_envelope->>'visibility') not in ('private','shared')
      or jsonb_typeof(p_envelope->'requires_reply') is distinct from 'boolean'
      or (p_envelope->>'created_at') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$'
      or jsonb_typeof(p_envelope->'artifact_refs') is distinct from 'array' then
    raise exception 'invalid envelope values';
  end if;
  -- Parse as well as checking the shape: impossible dates must fail.
  perform (p_envelope->>'created_at')::timestamptz;
  foreach k in array array['task_id','reply_to_message_id'] loop
    if p_envelope ? k and jsonb_typeof(p_envelope->k) <> 'null' then
      if jsonb_typeof(p_envelope->k) <> 'string' or length(p_envelope->>k) not between 1 and 200 then
        raise exception 'invalid threading field';
      end if;
    end if;
  end loop;
  if jsonb_array_length(p_envelope->'artifact_refs')>10 then raise exception 'too many artifacts'; end if;
  for ref in select value from jsonb_array_elements(p_envelope->'artifact_refs') loop
    if jsonb_typeof(ref) <> 'object' or not (ref ?& array['kind','uri','label']) then
      raise exception 'invalid artifact';
    end if;
    foreach k in array array['kind','uri','label'] loop
      if jsonb_typeof(ref->k) is distinct from 'string' then raise exception 'invalid artifact type'; end if;
    end loop;
    if (ref->>'kind') not in ('drive_file','drive_doc','github_pr','github_commit','repo_snapshot')
        or (ref->>'uri') !~ '^https://[^[:space:]]+$'
        or length(ref->>'uri')>2048 or length(btrim(ref->>'label')) not between 1 and 200 then
      raise exception 'invalid artifact values';
    end if;
    if ref->>'kind' in ('drive_file','repo_snapshot') and
        coalesce(ref->>'sha256','') !~ '^[0-9a-f]{64}$' then raise exception 'artifact checksum required'; end if;
    if ref->>'kind' = 'repo_snapshot' and
        coalesce(ref->>'base_commit','') !~ '^[0-9a-f]{40}$' then raise exception 'snapshot commit required'; end if;
    for k in select jsonb_object_keys(ref) loop
      if k <> all(array['kind','uri','label','sha256','base_commit','version','drive_id']) then
        raise exception 'unknown artifact field';
      end if;
      if jsonb_typeof(ref->k) <> 'string' or length(ref->>k)>2048 then
        raise exception 'invalid artifact metadata';
      end if;
    end loop;
    if ref ? 'sha256' and (ref->>'sha256') !~ '^[0-9a-f]{64}$' then raise exception 'bad checksum'; end if;
    if ref ? 'base_commit' and (ref->>'base_commit') !~ '^[0-9a-f]{40}$' then raise exception 'bad commit'; end if;
  end loop;

  insert into public.bus_messages
    (message_id,sender,recipient,topic,task_id,reply_to_message_id,status,requires_reply,payload)
  values (p_envelope->>'message_id',node,p_envelope->>'recipient',p_envelope->>'topic',
    p_envelope->>'task_id',p_envelope->>'reply_to_message_id','waiting',
    (p_envelope->>'requires_reply')::boolean,
    jsonb_build_object('body',p_envelope->>'summary' || E'\n\n— ' || upper(node),
                       'via','bus-v2','envelope',p_envelope))
  on conflict (message_id) do nothing returning message_id into inserted_id;
  if inserted_id is null then
    select * into prior from public.bus_messages where message_id=p_envelope->>'message_id';
    if prior.sender is distinct from node or prior.payload->'envelope' is distinct from p_envelope then
      raise exception 'message ID collision: immutable content differs' using errcode='23505';
    end if;
  end if;
  return jsonb_build_object('message_id',p_envelope->>'message_id','duplicate',inserted_id is null);
end;
$$;

create function public.ldi_inbox_v2(p_token text, p_limit integer default 20)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare node text; result jsonb;
begin
  node := public._ldi_who(p_token);
  if node is null then raise exception 'bad token' using errcode='28000'; end if;
  if p_limit is null or p_limit not between 1 and 100 then raise exception 'limit must be 1..100'; end if;
  -- Read only. No row, including rows outside the returned batch, is acknowledged here.
  select coalesce(jsonb_agg(to_jsonb(batch) order by batch.created_at,batch.message_id),'[]'::jsonb)
    into result from (
      select m.message_id,m.sender,m.recipient,m.topic,m.task_id,m.reply_to_message_id,
        m.status,m.requires_reply,m.payload,m.created_at,
        r.delivered_at,r.processed_at
      from public.bus_messages m left join public.bus_receipts_v2 r
        on r.message_id=m.message_id and r.recipient=node
      where m.recipient in (node,'all') and m.status not in ('done','rejected')
        and r.processed_at is null
      order by m.created_at,m.message_id limit p_limit
    ) batch;
  return result;
end;
$$;

create function public.ldi_ack_v2(p_token text,p_message_id text,p_state text)
returns jsonb language plpgsql security definer set search_path = '' as $$
declare node text; dest text;
begin
  node := public._ldi_who(p_token);
  if node is null then raise exception 'bad token' using errcode='28000'; end if;
  if p_state is null or p_state not in ('delivered','processed') then raise exception 'invalid receipt'; end if;
  select recipient into dest from public.bus_messages where message_id=p_message_id;
  if dest is null or dest not in (node,'all') then raise exception 'message not in node inbox' using errcode='42501'; end if;
  insert into public.bus_receipts_v2(message_id,recipient,processed_at)
    values(p_message_id,node,case when p_state='processed' then now() else null end)
    on conflict(message_id,recipient) do update set
      processed_at=coalesce(public.bus_receipts_v2.processed_at,excluded.processed_at);
  -- Keep legacy clients interoperable for individual messages. Broadcast receipts are per node.
  update public.bus_messages set status=case when p_state='processed' then 'done' else 'delivered' end
    where message_id=p_message_id and recipient=node
      and status not in ('done','rejected');
  return jsonb_build_object('message_id',p_message_id,'receipt',p_state);
end;
$$;

revoke all on function public.ldi_send_v2(text,jsonb) from public;
revoke all on function public.ldi_inbox_v2(text,integer) from public;
revoke all on function public.ldi_ack_v2(text,text,text) from public;
grant execute on function public.ldi_send_v2(text,jsonb) to anon,authenticated,service_role;
grant execute on function public.ldi_inbox_v2(text,integer) to anon,authenticated,service_role;
grant execute on function public.ldi_ack_v2(text,text,text) to anon,authenticated,service_role;
commit;
