#!/usr/bin/env python3
"""Push/pull system for cloud synchronization

Usage:
    dat init [--profile=<profile>] [<bucket>]
    dat checkin <file>
    dat checkout <file>
    dat clone [--profile=<profile>] <bucket>
    dat clone [--profile=<profile>] <bucket> <folder>
    dat delete
    dat [-d] pull
    dat [-d] push
    dat stash
    dat stash pop
    dat stash pop --hard
    dat [-r] status

Arguments:
    bucket     Name of bucket (ex: my-bucket)
    folder     Name of local folder
    -d         Dry run?
    -r         Check status against remote?
    --hard     Overwrites existing files when popping stash

Options:
    profile    Named profile to be passed to aws cli
"""

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
import platform
import subprocess
from glob import glob
from botocore.exceptions import ClientError
from docopt import docopt

def dat():
    arg = docopt(__doc__)
    if arg['init']: dat_init(arg['<bucket>'], arg['--profile'])
    elif arg['checkin']: dat_checkin(arg['<file>'])
    elif arg['checkout']: dat_checkout(arg['<file>'])
    elif arg['clone']: dat_clone(arg['<bucket>'], arg['<folder>'], arg['--profile'])
    elif arg['delete']: dat_delete()
    elif arg['push']: dat_push(arg['-d'])
    elif arg['pull']: dat_pull(arg['-d'])
    elif arg['stash']:
        if arg['pop']:
            dat_pop(arg['--hard'])
        else:
            dat_stash()
    elif arg['status']: dat_status(arg['-r'])

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

def take_inventory(config):
    inv = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            inv.append(re.sub('^\\./', '', root + '/' + file))
    inv = [x for x in inv if not x.startswith('.dat') and not x.startswith('.git')]
    out = dict()
    for f in inv:
        out[f] = md5(f)
    return out

def write_inventory(x, fname):
    f = open(fname, 'w')
    for d in sorted(x.keys()):
        f.write(d + '\t' + x[d] + '\n')
    f.close()

def read_inventory(fname = '.dat/local'):
    if os.path.isfile(fname):
        f = open(fname)
        out = dict()
        for line in f:
            row = line.strip().split('\t')
            out[row[0]] = row[1]
    else:
        out = {}
    return out

def read_config(filename='.dat/config'):
    if not os.path.isfile(filename):
        sys.exit(red(f'Not a dat repository; {filename} does not exit'))

    config = {}
    for line in open(filename):
        y = [x.strip() for x in line.split(':')]
        config[y[0]] = y[1]
    return config

def write_config(config, filename='.dat/config'):
    config_file = open(filename, 'w')
    for k in sorted(config.keys()):
        config_file.write(f"{k}: {config[k]}\n")
    config_file.close()

def get_master(config, local=None):
    if 'aws' in config.keys():

        # Try to get master
        cmd = f"aws s3 cp s3://{config['aws']}/.dat/master .dat/master"
        if 'profile' in config.keys(): cmd = cmd + f" --profile {config['profile']}"
        a = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        if os.path.isfile('.dat/master'):
            # download successful
            master = read_inventory('.dat/master')
            os.remove('.dat/master')
        elif config['pushed'] == 'False':
            # create bucket
            if 'profile' in config.keys():
                boto3.setup_default_session(profile_name=config['profile'])
            s3 = boto3.client('s3')
            bucket = config['aws'].split('/')[0]
            s3.create_bucket(Bucket=bucket)
            master = local.copy()
        else:
            # something went wrong
            quit(red('Bucket exists (according to config) but cannot be accessed; are you logged in?'))
    else:
        sys.exit(red('Only aws pulls are supported in this version'))
    return master

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
    return push

def needs_pull(master, local):
    pull = set()
    for f in master.keys():
        if f not in local.keys():
            pull.add(f)
        elif local[f] != master[f]:
            pull.add(f)
    return pull

def needs_purge(current, local):
    purge = set()
    for f in local.keys():
        if f not in current.keys():
            purge.add(f)
    return purge

def needs_kill(master, local):
    kill = set()
    for f in local.keys():
        if f not in master.keys():
            kill.add(f)
    return kill

def resolve_push_conflicts(current, local, master, push, hard=True):
    conflict = set()
    resolved = set()
    for f in push:
        if f in local.keys():
            if f in master.keys():
                if master[f] == local[f]:
                    if hard:
                        master[f] = current[f] # Good for push
                        local[f] = current[f]  # Good for push
                elif master[f] == current[f]:
                    local[f] = current[f]  # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                master[f] = current[f]  # Remote deletion, but go ahead and push new
                local[f] = current[f]   # Remote deletion, but go ahead and push new
        else:
            if f in master.keys():
                if master[f] == current[f]:
                    local[f] = current[f]  # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                master[f] = current[f]  # Brand new file
                local[f] = current[f]
    return [conflict, resolved]

def resolve_purge_conflicts(master, local, purge, hard=True):
    conflict = set()
    resolved = set()
    for f in purge:
        if f in master.keys():
            if master[f] != local[f]:
                conflict.add(f)
            elif hard:
                master.pop(f)  # OK, go ahead with purge
                local.pop(f)
        else:
            local.pop(f)  # Handle quietly; just fix local
            resolved.add(f)
    return [conflict, resolved]

def resolve_pull_conflicts(current, local, master, pull, hard=True):
    conflict = set()
    resolved = set()
    for f in pull:
        if f in local.keys():
            if f in current.keys():
                if current[f] == local[f]:
                    if hard: local[f] = master[f]  # Good
                elif current[f] == master[f]:
                    local[f] = master[f]  # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                conflict.add(f)  # Deleted locally, changed remotely
        else:
            if f in current.keys():
                if current[f] == master[f]:
                    local[f] = master[f]  # OK, resolve locally
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                local[f] = master[f]  # Good
    return [conflict, resolved]

def resolve_kill_conflicts(current, local, kill, hard=True):
    conflict = set()
    resolved = set()
    for f in kill:
        if f in current.keys():
            if current[f] != local[f]:
                conflict.add(f)
            elif hard:
                local.pop(f)  # OK, go ahead with purge
        else:
            local.pop(f)  # OK, handle quietly
            resolved.add(f)
    return [conflict, resolved]

def dat_checkin(filename):

    # Read in config file
    config = read_config()

    # Update manifest
    current = take_inventory(config)
    local = read_inventory('.dat/local')
    local[filename] = current[filename]
    write_inventory(local, '.dat/local')
    master = get_master(config, local)
    master[filename] = current[filename]

    # Push file, master
    cmd = f'''aws s3 sync --no-follow-symlinks ./ s3://{config['aws']}/ --exclude "*" --include ".dat/master" --include "{filename}"'''
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    try:
        os.system(cmd)
    except:
        quit(red('Failed to push file; are you logged in?'))

def dat_checkout(filename):

    # Read in config file
    config = read_config()

    # Parse filename
    fd = os.path.dirname(filename)
    if fd =='': fd = '.'
    ff = os.path.basename(filename)
    dest = fd + '/' + ff

    # Pull file
    cmd = f"aws s3 cp s3://{config['aws']}/{filename} {dest}"
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    try:
        os.system(cmd)
    except:
        quit(red('Failed to pull file; are you logged in?'))

    # Update manifest
    current = take_inventory(config)
    local = read_inventory('.dat/local')
    local[filename] = current[filename]
    write_inventory(local, '.dat/local')

def dat_clone(bucket, folder, profile=None):

    # Process bucket
    if folder is None: folder = bucket
    if ':' not in bucket:
        loc = 'aws'
        id = bucket
    else:
        [loc, id] = bucket.split(':')

    # Create folder
    if os.path.isdir(folder): sys.exit(red(f'Error: Directory "{folder}" already exists'))
    os.mkdir(folder)

    # Clone
    if loc == 'aws':
        cmd = 'aws s3 sync s3://' + id + '/ ' + folder + '/'
        if profile is not None:
            cmd = cmd + f' --profile {profile}'
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
    config = {'pushed': 'True'}
    config[loc] = id
    if profile is not None: config['profile'] = profile
    write_config(config, f'{folder}/.dat/config')

    # Convert if old-style dat format
    if os.path.isfile(folder + '/.dat/master'):
        os.rename(folder + '/.dat/master', folder + '/.dat/local')
    else:
        print('Warning: No .dat/master file -- upgrade dat version to md5')

def dat_delete():

    # Read in config file
    config = read_config()

    # Delete remote files (+ bucket)
    cmd = f"aws s3 rm s3://{config['aws']} --recursive"
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    if '/' not in config['aws']:
        if 'profile' in config.keys():
            session = boto3.Session(profile_name=config['profile'])
            s3 = session.client('s3')
        else:
            s3 = boto3.client('s3')

        try:
            all_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        except ClientError:
            quit(red('Token has expired; run "aws login"'))

        if config['aws'] in all_buckets:
            os.system(cmd)
            s3.delete_bucket(Bucket=config['aws'])
            print(f"Deleted aws bucket: {config['aws']}")
        else:
            quit(red(f"Bucket {config['aws']} does not exist"))

    # Delete .dat folder
    shutil.rmtree('.dat')

def dat_init(id, profile):

    # Don't overwrite existing config
    if os.path.isdir('.dat'):
        exit(red('Error: .dat directory already exists'))
    else:
        os.mkdir('.dat')

    # Create id
    if id is None:
        username = os.environ.get('USERNAME') or os.environ.get('USER')
        id = f"{username}.{os.getcwd().replace(os.environ['HOME'], '').strip('/').replace('/', '.').lower()}"

    # Write config file
    config = open('.dat/config', 'w')
    config.write(f'aws: {id}\n')
    config.write(f'pushed: False\n')
    if profile is not None:
        config.write(f'profile: {profile}\n')
        print(green(f'Configured for profile={profile} aws bucket: ') + id)
    else:
        print(green('Configured for aws bucket: ') + id)
    config.close()

def dat_pull(dry=False):

    # Read in config file
    config = read_config()

    # Get master/current/local
    current = take_inventory(config)
    local = read_inventory('.dat/local')
    master = get_master(config)

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
        opt = '--delete --exclude "*"'
        for f in sorted((pull | kill) - pull_conflict - kill_conflict - pull_resolved - kill_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        cmd = f"aws s3 sync s3://{config['aws']} . {opt}"
        if 'profile' in config.keys():
            cmd = cmd + f" --profile {config['profile']}"
        if dry:
            print(cmd)
            print('Resolved: ' + str(resolved))
        else:
            os.system(cmd)
            write_inventory(local, '.dat/local')
    elif len(conflict) == 0:
        if dry:
            print('--no command issued--')
        else:
            write_inventory(local, '.dat/local')
        exit('Everything up-to-date')

def dat_push(dry=False):

    # Read in config file
    config = read_config()

    # Get current/local
    current = take_inventory(config)
    local = read_inventory('.dat/local')

    # Create push, purg lists
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    # Either exit or get master
    if len(push | purg) == 0:
        exit('Everything up-to-date')
    else:
        master = get_master(config, local)

    # Check for conflicts
    [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push)
    [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg)
    conflict = sorted(push_conflict | purg_conflict)
    if len(conflict) > 0:
        print(red("Unable to push the following files: conflict with master\n" + '\n'.join(conflict)))

    # Sync
    resolved = sorted(push_resolved | purg_resolved)
    if len(push | purg):
        opt = '--delete --exclude "*" --include .dat/master'
        for f in sorted((push | purg) - push_conflict - purg_conflict - push_resolved - purg_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        if 'profile' in config.keys():
            opt = opt + f" --profile {config['profile']}"
        cmd = f"aws s3 sync --no-follow-symlinks . s3://{config['aws']} {opt}"
        if dry:
            print(cmd)
            print('Resolved: ' + str(resolved))
        else:
            write_inventory(master, '.dat/master')
            os.system(cmd)
            write_inventory(local, '.dat/local')
            os.remove('.dat/master')
    elif len(conflict) == 0:
        if not dry: write_inventory(local, '.dat/local')
        exit('Everything up-to-date')

    # Remove never pushed tag, if present
    if not dry:
        config['pushed'] = 'True'
        write_config(config)

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
    config = read_config()

    # Check for existing stash
    if os.path.isdir('.dat/stash'): exit('Error: Unpopped stash detected!')

    # Get master/current/local
    current = take_inventory(config)
    local = read_inventory('.dat/local')
    master = get_master(config)
    if len(local) == 0: local = current

    # Create conflict list
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = pull_conflict.union(kill_conflict)

    # Stash conflicted files
    os.mkdir('.dat/stash')
    for f in conflict:
        shutil.move(f, '.dat/stash/')
        local.pop(f)
        write_inventory(local, '.dat/local')

def dat_status(remote):

    # Read in config file
    config = read_config()

    # Get current/local
    current = take_inventory(config)
    local = read_inventory('.dat/local')

    if config['pushed'] == 'False':
        print(red('dat initialized, but never pushed'))

    # Create push, purg lists
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if remote:
        master = get_master(config)
        olocal = local.copy()
        omaster = master.copy()

        # Check that repo is current
        pull = needs_pull(master, local)
        kill = needs_kill(master, local)

        # Check for conflicts
        [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push, hard=False)
        master = omaster.copy()
        write_inventory(local, '.dat/local')
        local = olocal.copy()
        [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg, hard=False)
        master = omaster.copy()
        write_inventory(local, '.dat/local')
        local = olocal.copy()
        [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull, hard=False)
        master = omaster.copy()
        write_inventory(local, '.dat/local')
        local = olocal.copy()
        [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill, hard=False)
        write_inventory(local, '.dat/local')

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
            if config['pushed'] == 'True':
                print(red('Local dat empty; never been pulled?'))
        else:
            if len(push | purg) > 0:
                if len(push) > 0:
                    print(blue('Modified locally: \n  ') + '\n  '.join(sorted(push)))
                if len(purg) > 0:
                    print(blue('Deleted locally: \n  ') + '\n  '.join(sorted(purg)))
            else:
                print(green('Nothing to push; local is clean'))
