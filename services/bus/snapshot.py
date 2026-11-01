"""Create a complete deterministic main snapshot, pinned to a Git commit.

Reads tracked blobs only. Does not read working-tree secrets or push/upload anything.
"""
import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path


def git(repo,*args):
    return subprocess.check_output(['git','-C',str(repo),*args])


def build(repo,revision,output):
    commit=git(repo,'rev-parse','--verify','--end-of-options',revision+'^{commit}').decode().strip()
    tree=git(repo,'ls-tree','-rz','--full-tree',commit)
    rows=[]
    for entry in tree.split(b'\0'):
        if not entry:continue
        fields,name=entry.split(b'\t',1);mode,kind,oid=fields.decode().split()
        if kind!='blob':raise ValueError('Submodules need explicit snapshot policy; refusing partial archive')
        name=name.decode('utf-8')
        if name=='SNAPSHOT.json' or name.startswith('/') or '..' in Path(name).parts:
            raise ValueError('unsafe/reserved snapshot path')
        rows.append((name,mode,oid))
    manifest={'schema_version':'1','base_commit':commit,'files':{}}
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
    temp=output.with_name(output.name+'.partial')
    try:
        with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
            for name,mode,oid in sorted(rows):
                data=git(repo,'cat-file','blob',oid)
                manifest['files'][name]={'sha256':hashlib.sha256(data).hexdigest(),'git_mode':mode,'bytes':len(data)}
                info=zipfile.ZipInfo(name,(1980,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
                info.create_system=3;info.external_attr=int(mode,8)<<16
                z.writestr(info,data)
            info=zipfile.ZipInfo('SNAPSHOT.json',(1980,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,json.dumps(manifest,sort_keys=True,separators=(',',':')).encode())
        temp.replace(output)
    finally:temp.unlink(missing_ok=True)
    return {'file':str(output),'base_commit':commit,'files':len(rows),
            'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'bytes':output.stat().st_size}


def verify(path):
    with zipfile.ZipFile(path) as z:
        names=z.namelist()
        if len(names)!=len(set(names)):raise ValueError('duplicate snapshot entries')
        m=json.loads(z.read('SNAPSHOT.json'))
        if set(names)!={'SNAPSHOT.json',*m['files']}:
            raise ValueError('snapshot file census mismatch')
        for name,meta in m['files'].items():
            if name.startswith('/') or '..' in Path(name).parts:raise ValueError('unsafe snapshot path')
            if hashlib.sha256(z.read(name)).hexdigest()!=meta['sha256']:
                raise ValueError('snapshot content mismatch: '+name)
    return {'base_commit':m['base_commit'],'files':len(m['files']),'verified':True}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    b=sub.add_parser('build');b.add_argument('--repo',required=True);b.add_argument('--revision',default='origin/main');b.add_argument('--output',required=True)
    v=sub.add_parser('verify');v.add_argument('file')
    a=p.parse_args()
    print(json.dumps(build(a.repo,a.revision,a.output) if a.command=='build' else verify(a.file)))
