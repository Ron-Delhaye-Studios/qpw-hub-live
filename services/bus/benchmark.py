"""Measure a private sample; output aggregate metrics only, never source messages."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from brief import BriefStore,render_packet


def measure(rows,budget=6000):
    raw=json.dumps(rows,ensure_ascii=False,separators=(',',':'))
    with tempfile.TemporaryDirectory() as tmp:
        store=BriefStore(Path(tmp)/'private.sqlite')
        first=render_packet(rows,budget=budget,store=store);pages=[first]
        while pages[-1]['shown_ids']:
            store.ack(pages[-1]['receipt'])
            pages.append(render_packet(rows,budget=budget,store=store))
            if len(pages)>len(rows)+1:raise ValueError('sample did not drain')
        counts=json.loads(subprocess.check_output(['node',str(Path(__file__).with_name('benchmark_tokens.mjs'))],
            input=json.dumps([raw,first['text'],pages[-1]['text']],ensure_ascii=False).encode()))
    raw_tokens,first_tokens,repeat_tokens=counts['tokens']
    return {'sample':'private local message sample; source text not included',
        'rows':len(rows),'budget_characters':budget,'raw_characters':len(raw),
        'first_brief_characters':len(first['text']),'first_shown':len(first['shown_ids']),
        'first_deferred':first['deferred'],'raw_tokens':raw_tokens,'first_tokens':first_tokens,
        'triage_token_reduction_percent':round(100*(1-first_tokens/raw_tokens),1) if raw_tokens else None,
        'pages_to_see_all':len(pages)-1,
        'all_messages_retained':sum(len(p['shown_ids']) for p in pages)==len({r['message_id'] for r in rows if r.get('status') not in ('done','rejected')}),
        'repeat_tokens':repeat_tokens,'repeat_seen_pending':pages[-1]['seen_pending'],
        'tokenizer':'js-tiktoken 1.0.21 / '+counts['encoding'],
        'limit':'Triage-input comparison only. Full-detail fetches cost extra. Reference encoding, not universal model billing or end-to-end cost savings.'}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('sample');p.add_argument('--output',required=True)
    p.add_argument('--budget-chars',type=int,default=6000);a=p.parse_args()
    report=measure(json.loads(Path(a.sample).read_text()),a.budget_chars)
    Path(a.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
