# Definitions:
#   push: local file is changed/new
#   pull: remote file is changed/new
#   purge: local file has been deleted (remove from master?)
#   kill: remote file has been deleted (remove from current?)

# Setup
import os
import re
import sys
import boto3
import shutil
import hashlib
import manifest
import platform
import subprocess
from glob import glob
from boto3.session import botocore
from botocore.exceptions import ClientError

# ANSI escape sequences
def red(x): return '\033[01;38;5;196m' + x + '\033[0m'
def green(x): return '\033[01;38;5;46m' + x + '\033[0m'
def blue(x): return '\033[01;38;5;39m' + x + '\033[0m'

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def take_inventory():
    base = '_site' if os.path.isdir('_site') else '.'
    inv = []
    for root, dirs, files in os.walk(base):
        for file in files:
            inv.append(re.sub('^\\./', '', root + '/' + file))
    inv = [x for x in inv if x != '.dat-config' and x != '.gitignore' and x != '.dat-stash' and x != '.dat-local' and x != '.dat-master']
    out = dict()
    for f in inv:
        out[f] = md5(f)
    return out

def write_inventory(x, fname):
    f = open(fname, 'w')
    for d in sorted(x.keys()):
        f.write(d + '\t' + x[d] + '\n')
    f.close()

def read_inventory(fname):
    if os.path.isfile('.dat-local'):
        f = open(fname)
        out = dict()
        for line in f:
            row = line.strip().split('\t')
            out[row[0]] = row[1]
    else:
        out = {}
    return out

def read_config(push_check=False):
    if os.path.isfile('.dat-config'):
        config = open('.dat-config', 'r')
        [loc,id] = config.readline().rstrip().split(':')
        if len(id) == 0:
            sys.exit(".dat-config file is empty")
    else:
        sys.exit('Not a dat repository; no .dat-config file')
    if push_check:
        if config.readline() == 'NEVER PUSHED\n':
            config.close()
            return([loc, id, False])
        else:
            config.close()
            return([loc, id, True])
    else:
        config.close()
        return([loc, id])

def get_master(loc, id, local=None):
    if loc == 'aws':
        if 'profile: personal' in open('.dat-config').read():
            personal = True
            boto3.setup_default_session(profile_name='personal')
        else:
            personal = False
        s3 = boto3.client('s3')
        try:
            allBuckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        except ClientError:
            quit(red('Token has expired; run "aws login"'))
        bucket = id.split('/')[0]
        if bucket in allBuckets:
            base = '_site' if os.path.isdir('_site') else '.'
            cmd = 'aws s3 cp s3://' + id + '/.dat-master ' + base
            if personal: cmd = cmd + ' --profile personal'
            a = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            if os.path.isfile(base + '/.dat-master'):
                master = read_inventory(base + '/.dat-master')
                os.remove(base + '/.dat-master')
            else:
                master = local
        else:
            if local is not None:
                s3.create_bucket(Bucket=bucket)
                master = local
            else:
                print(red('Remote bucket not created yet'))
                sys.exit(1)
    else:
        sys.exit('Only aws pulls are supported')
    return(master)

def needs_push(current, local):
    push = set()
    if len(local):
        for f in current.keys():
            if f not in local.keys():
                push.add(f)
            elif current[f] != local[f]:
                push.add(f)
    else:
        for f in current.keys():
            push.add(f)
    return(push)

def needs_pull(master, local):
    pull = set()
    for f in master.keys():
        if f not in local.keys():
            pull.add(f)
        elif local[f] != master[f]:
            pull.add(f)
    return(pull)

def needs_purge(current, local):
    purge = set()
    for f in local.keys():
        if f not in current.keys():
            purge.add(f)
    return(purge)

def needs_kill(master, local):
    kill = set()
    for f in local.keys():
        if f not in master.keys():
            kill.add(f)
    return(kill)

def resolve_push_conflicts(current, local, master, push):
    conflict = set()
    resolved = set()
    for f in push:
        if f in local.keys():
            if f in master.keys():
                if master[f] == local[f]:
                    master[f] = current[f] # Good
                    local[f] = current[f]
                elif master[f] == current[f]:
                    local[f] = current[f] # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                master[f] = current[f] # Remote deletion, but go ahead and push new
                local[f] = current[f] # Remote deletion, but go ahead and push new
        else:
            if f in master.keys():
                if master[f] == current[f]:
                    local[f] = current[f] # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                master[f] = current[f] # Brand new file
                local[f] = current[f]
    return([conflict, resolved])

def resolve_purge_conflicts(master, local, purge):
    conflict = set()
    resolved = set()
    for f in purge:
        if f in master.keys():
            if master[f] == local[f]:
                master.pop(f) # OK, go ahead with purge
                local.pop(f)
            else:
                conflict.add(f)
        else:
            local.pop(f) # Handle quietly; just fix local
            resolved.add(f)
    return([conflict, resolved])

def resolve_pull_conflicts(current, local, master, pull):
    conflict = set()
    resolved = set()
    for f in pull:
        if f in local.keys():
            if f in current.keys():
                if current[f] == local[f]:
                    local[f] = master[f] # Good
                elif current[f] == master[f]:
                    local[f] = master[f] # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                conflict.add(f)  # Deleted locally, changed remotely
        else:
            if f in current.keys():
                if current[f] == master[f]:
                    local[f] = master[f]
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                local[f] = master[f]  # Good
    return([conflict, resolved])

def resolve_kill_conflicts(current, local, kill):
    conflict = set()
    resolved = set()
    for f in kill:
        if f in current.keys():
            if current[f] == local[f]:
                local.pop(f)  # OK, go ahead with purge
            else:
                conflict.add(f)
        else:
            local.pop(f)
            resolved.add(f)
    return([conflict, resolved])

def dat_checkout(filename):

    # Read in config file
    [loc, id] = read_config()

    # Parse filename
    fd = os.path.dirname(filename)
    if fd =='': fd = '.'
    ff = os.path.basename(filename)
    dest = fd + '/' + ff

    # Pull file
    cmd = 'aws s3 cp s3://' + id + '/' + filename + ' ' + dest
    if 'profile: personal' in open('.dat-config').read():
        cmd = cmd + ' --profile personal'
        personal = True
        boto3.setup_default_session(profile_name='personal')
    else:
        personal = False
    s3 = boto3.client('s3')
    try:
        allBuckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
    except ClientError:
        quit(red('Token has expired; run "aws login"'))
    os.system(cmd)

    # Update manifest
    current = take_inventory()
    local = read_inventory('.dat-local')
    local[filename] = current[filename]
    write_inventory(local, '.dat-local')

def dat_clone(bucket, folder):

    # Process bucket
    if bucket is None: bucket = 'aws:pbreheny.' + os.getcwd().replace(os.environ['HOME'], '').strip('/').replace('/', '.').lower() + '.' + folder.lower()
    if ':' not in bucket: exit('Error: Central location must be of form aws:id or hpc:id')
    [loc, id] = bucket.split(':')

    # Create folder
    if os.path.isdir(folder): exit('Error: Directory "' + folder + '" already exists')
    os.mkdir(folder)

    # Clone
    if loc == 'aws':
        cmd = 'aws s3 sync s3://' + id + '/ ' + folder + '/'
        if os.path.isfile('.dat-config'):
            if 'profile: personal' in open('.dat-config').read(): cmd = cmd + ' --profile personal'
        else:
            cmd = cmd + ' --profile personal' # Need a better solution to this
        err = os.system(cmd)
    elif loc == 'hpc':
        if 'argon' in platform.node():
            hub = '/Shared/Fisher/hub/'
        elif os.path.isdir(os.environ['HOME'] + '/lss'):
            hub = os.environ['HOME'] + '/lss/Fisher/hub/'
        else:
            hub = 'hpc-data:/Shared/Fisher/hub/'
        err = os.system('rsync -avz ' + hub + id + '/ ' + folder + '/')
    else:
        err = 1
        print('Error: Central location must be of form aws:id or hpc:id')
    if err:
        os.rmdir(folder)
        exit()

    # Write config
    f = open(folder + '/.dat-config', 'w')
    f.write(loc + ':' + id + '\n')

    # Convert if old-style dat format
    if os.path.isfile(folder + '/.dat-master'):
        os.rename(folder + '/.dat-master', folder + '/.dat-local')
    else:
        print('Warning: No .dat-master file -- upgrade dat version to md5')

def dat_delete(personal=False):

    # Read in config file
    [loc, id, pushed] = read_config(True)
    sub = id.startswith('pbreheny.public.web.host/')

    # Delete remote files (+ bucket)
    if id.startswith('pbreheny.public.web.host/'):
        cmd = 'aws s3 rm s3://' + id + ' --recursive'
        os.system(cmd)
    else:
        if 'profile: personal' in open('.dat-config').read(): personal=True
        if personal:
            session = boto3.Session(profile_name='personal')
            s3 = session.client('s3')
        else:
            s3 = boto3.client('s3')
        try:
            all_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        except ClientError:
            quit(red('Token has expired; run "aws login"'))
        if id in all_buckets:
            # Delete all objects from bucket
            cmd = 'aws s3 rm s3://' + id + ' --recursive'
            if personal:
                cmd = cmd + ' --profile personal'
            os.system(cmd)

            # Delete bucket
            s3.delete_bucket(Bucket=id)
        else:
            quit(red('Bucket ' + id + ' does not exist'))

    # Local
    if os.path.isfile('.dat-local'): os.remove('.dat-local')
    os.system('echo "NEVER PUSHED" >> .dat-config')

def dat_pull(dry=False):

    # Read in config file
    [loc, id] = read_config()

    # Get master/current/local
    current = take_inventory()
    local = read_inventory('.dat-local')
    master = get_master(loc, id)

    # Create pull, purge lists
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)

    # Check for conflicts
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = sorted(pull_conflict | kill_conflict)
    if len(conflict) > 0:
        print(red("Unable to pull the following files: conflict with current\n  " + '\n  '.join(conflict)))

    # Sync
    resolved = sorted(kill_resolved | pull_resolved)
    if len(pull | kill):
        opt = ' --delete --exclude "*"'
        for f in sorted((pull | kill) - pull_conflict - kill_conflict - pull_resolved - kill_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        base = '_site' if os.path.isdir('_site') else '.'
        cmd = 'aws s3 sync s3://' + id + ' ' + base + opt
        if 'profile: personal' in open('.dat-config').read():
            cmd = cmd + ' --profile personal'
        if dry:
            print(cmd)
            print('Resolved: ' + str(resolved))
        else:
            os.system(cmd)
            write_inventory(local, '.dat-local')
    elif len(conflict) == 0:
        if dry:
            print('--no command issued--')
        else:
            write_inventory(local, '.dat-local')
        exit('Everything up-to-date')

def dat_push(dry=False):

    # Read in config file
    [loc, id, pushed] = read_config(True)

    # Get current/local
    current = take_inventory()
    local = read_inventory('.dat-local')

    # Create push, purg lists
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    # Either exit or get master
    if len(push | purg) == 0:
        exit('Everything up-to-date')
    else:
        master = get_master(loc, id, local)

    # Check for conflicts
    [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push)
    [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg)
    conflict = sorted(push_conflict | purg_conflict)
    if len(conflict) > 0:
        print(red("Unable to push the following files: conflict with master\n" + '\n'.join(conflict)))

    # Sync
    resolved = sorted(push_resolved | purg_resolved)
    if len(push | purg):
        opt = ' --delete --exclude "*" --include .dat-master'
        for f in sorted((push | purg) - push_conflict - purg_conflict - push_resolved - purg_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        if 'profile: personal' in open('.dat-config').read():
            opt = opt + ' --profile personal'
        base = '_site' if os.path.isdir('_site') else '.'
        cmd = 'aws s3 sync --no-follow-symlinks ' + base + ' s3://' + id + opt
        if dry:
            print(cmd)
            print('Resolved: ' + str(resolved))
        else:
            write_inventory(master, base + '/.dat-master')
            os.system(cmd)
            write_inventory(local, '.dat-local')
            os.remove(base + '/.dat-master')
    elif len(conflict) == 0:
        if not dry: write_inventory(local, '.dat-local')
        exit('Everything up-to-date')

    # Remove never pushed tag, if present
    if not dry:
        config = open('.dat-config').read().split('\n')
        if 'NEVER PUSHED' in config:
            config.remove('NEVER PUSHED')
            f = open('.dat-config', 'w')
            for line in config:
                if line != '': f.write(line + '\n')
            f.close()
def dat_pop(hard=False):
    if not os.path.isdir('.dat/stash'): exit('Error: No stash detected!')
    for f in glob(r'.dat/stash/*'):
        ff = os.path.basename(f)
        if os.path.isfile(f):
            if hard:
                shutil.move(f, './' + ff)
            else:
                exit('Popping stash would overwrite file ' + ff + '.\nIf you wish to overwrite existing files, rerun with \ndat stash pop --hard')
        else:
            shutil.move(f, '.')
    os.rmdir('.dat/stash')
    return()

def dat_stash():

    # Read in config file
    [loc, id] = read_config()

    # Check for existing stash
    if os.path.isdir('.dat/stash'): exit('Error: Unpopped stash detected!')

    # Get master/current/local
    current = take_inventory()
    local = read_inventory('.dat-local')
    master = get_master(loc, id)
    if len(local) == 0: local = current

    # Create conflict list
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = pull_conflict.union(kill_conflict)

    #push = needs_push(current, local)
    #purge = needs_purge(current, local)
    #[push_conflict, push_resolved] = resolve_push_conflicts(master, local, push)
    #[purge_conflict, purge_resolved] = resolve_purge_conflicts(master, local, purge)
    #conflict = pull_conflict.union(kill_conflict)

    # Stash conflicted files
    if not os.path.isdir('.dat'): os.mkdir('.dat')
    os.mkdir('.dat/stash')
    for f in conflict:
        shutil.move(f, '.dat/stash/')
        local.pop(f)
        write_inventory(local, '.dat-local')

def dat_status(remote):

    # Run recursively if at project head
    if not os.path.isfile('.dat-config'):
        current_dir = os.getcwd()
        md = manifest.manifest_dats(current_dir).split(';')
        if len(md[0]) == 0:
            print('Not a dat repository!')
            sys.exit(1)
        else:
            for dat in md:
                dat_dir = dat.split('@')[0]
                if os.path.isdir(dat_dir):
                    print('\nSubdirectory: ' + blue(dat_dir))
                    os.chdir(dat_dir)
                    dat_status(True)
                    os.chdir(current_dir)
                else:
                    print('dat directory ' + red(dat_dir) + ' does not exist')
            print()
        exit()

    # Read in config file
    [loc, id, pushed] = read_config(True)

    # Get current/local
    current = take_inventory()
    local = read_inventory('.dat-local')

    if not pushed:
        print(red('dat initialized, but never pushed'))

    # Create push, purg lists
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if remote:
        master = get_master(loc, id)
        olocal = local.copy()
        omaster = master.copy()

        # Check that repo is current
        pull = needs_pull(master, local)
        kill = needs_kill(master, local)

        # Check for conflicts
        [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push)
        master = omaster.copy()
        write_inventory(local, '.dat-local')
        local = olocal.copy()
        [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg)
        master = omaster.copy()
        write_inventory(local, '.dat-local')
        local = olocal.copy()
        [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
        write_inventory(local, '.dat-local')
        local = olocal.copy()
        [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
        write_inventory(local, '.dat-local')

        # Report conflicts
        all_conflict = pull_conflict | push_conflict | purg_conflict | kill_conflict
        conflict = sorted(all_conflict - (kill_conflict & push))
        if len(conflict) > 0:
            print(red("Local/remote conflicts in the following files:\n  " + '\n  '.join(conflict)))

        # Report modifications
        a = sorted(pull - pull_conflict - pull_resolved)
        if len(a): print(blue('Modified remotely: \n  ') + '\n  '.join(a))
        b = sorted(push - push_conflict - kill_conflict - push_resolved)
        if len(b): print(blue('Modified locally: \n  ') + '\n  '.join(b))
        c = sorted(kill - kill_conflict - kill_resolved)
        if len(c): print(blue('Deleted remotely: \n  ') + '\n  '.join(c))
        d = sorted(purg - purg_conflict - purg_resolved)
        if len(d): print(blue('Deleted locally: \n  ') + '\n  '.join(d))
        e = sorted(kill_conflict & push)
        if len(e): print(blue('Deleted remotely but modified locally (can be pushed, but should it?): \n  ') + '\n  '.join(e))
        if len(a) + len(b) + len(c) + len(d) + len(conflict) == 0:
            print(green('Local is current with remote'))
    else:
        if len(local) == 0:
            if pushed:
                print(red('Local dat empty; never been pulled?'))
        else:
            if len(push | purg) > 0:
                if len(push) > 0:
                    print(blue('Modified locally: \n  ') + '\n  '.join(sorted(push)))
                if len(purg) > 0:
                    print(blue('Deleted locally: \n  ') + '\n  '.join(sorted(purg)))
            else:
                print(green('Nothing to push; local is clean'))
