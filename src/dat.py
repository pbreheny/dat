#!/usr/bin/env python3
"""
Push/pull system for cloud synchronization

Usage:
    dat init [--profile=<profile>] [<bucket>]
    dat checkin <file>
    dat checkout <file>
    dat clone [--profile=<profile>] <bucket> [<folder>]
    dat delete
    dat [-d] [-v] [--region=<region>] pull
    dat [-d] [-v] [--region=<region>] push
    dat stash
    dat stash pop [--hard]
    dat [-r] status
    dat overwrite-master
    dat repair-master
    dat share <account_number> [<username>] [--root] [-v]

Arguments:
    bucket           Name of the bucket (ex: my-bucket)
    folder           Name of local folder
    file             Name of the file to check in or out
    account_number   AWS account number associated with the IAM user
    username         IAM username to share the bucket with (omit if using --root)

Options:
    -d                       Dry run?
    -r                       Check status against remote?
    -v                       Verbose? (for debugging)
    --region=<region>        AWS region for the S3 bucket [default: us-east-1]
    --profile=<profile>      AWS CLI profile to use
    --hard                   Overwrite existing files when popping stash
    --root                   Share the bucket with the root account (omit <username> when using this)
"""

# Setup
import os
import re
import sys
import boto3
import json
import shutil
import hashlib
import platform
import subprocess
import textwrap
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
    elif arg['push']: dat_push(arg['-d'], arg['-v'], arg['--region'])
    elif arg['pull']: dat_pull(arg['-d'], arg['-v'], arg['--region'])
    elif arg['stash']:
        if arg['pop']:
            dat_pop(arg['--hard'])
        else:
            dat_stash()
    elif arg['status']: dat_status(arg['-r'])
    elif arg['overwrite-master']: dat_overwrite_master()
    elif arg['repair-master']: dat_repair_master()
    elif arg['share']:
        dat_share(
            arg['<account_number>'],
            arg['<username>'],
            root=arg['--root'],
            verbose=arg['-v']
        )


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
    inv = [x for x in inv if not x.startswith('.dat') and not x.startswith('.git') and not x == '.DS_Store']
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

    if os.path.isfile('.dat/local'):
        try:
            x = subprocess.run('git check-ignore .dat/local', shell=True, stdout=subprocess.PIPE).stdout.decode().strip()
            terminal_width = shutil.get_terminal_size().columns
            if x != '.dat/local':
                msg = 'Warning! You appear to be tracking .dat/local with git. This will almost certainly prevent dat from working correctly. Add'
                print(red(textwrap.fill(msg, width=terminal_width) + '\n**/.dat/local\nto your .gitignore file'))
        except:
            pass

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
        if 'profile' in config.keys():
            cmd = cmd + f" --profile {config['profile']}"
        a = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        if os.path.isfile('.dat/master'):
            # download successful
            master = read_inventory('.dat/master')
            os.remove('.dat/master')
        elif config['pushed'] == 'False':
            # create bucket
            if 'profile' in config.keys():
                boto3.setup_default_session(profile_name=config['profile'])
            s3 = boto3.client('s3', region_name=config.get('region', 'us-east-1'))
            bucket = config['aws'].split('/')[0]
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': config.get('region', 'us-east-1')
                }
            )
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
    if not os.path.isfile(filename): quit(red(f'"{filename}" does not exist'))
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
        write_inventory(master, '.dat/master')
        os.system(cmd)
        write_inventory(local, '.dat/local')
        os.remove('.dat/master')
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

def dat_overwrite_master():

    config = read_config()
    terminal_width = shutil.get_terminal_size().columns
    msg = 'Warning: This will completely replace the remote dat repository with your local copy. Are you sure you want to do this?'
    confirm = input(textwrap.fill(msg, width=terminal_width) + '\nPress (y) to confirm: ')
    if confirm != 'y': quit('Exiting...')

    # Take inventory
    current = take_inventory(config)
    write_inventory(current, '.dat/master')
    if os.path.isfile('.dat/local'): os.remove('.dat/local')

    # Overwrite
    cmd = f'''aws s3 sync --no-follow-symlinks --delete ./ s3://{config['aws']}/'''
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    try:
        os.system(cmd)
        write_inventory(current, '.dat/local')
        os.remove('.dat/master')
    except:
        quit(red('Failed to push file; are you logged in?'))

def dat_push(dry=False, verbose=False, region='us-east-1'):
    # Read in config file
    if verbose: print('Reading config')
    config = read_config()

    # Set the region
    config['region'] = region if region else config.get('region', 'us-east-1')

    # Get current/local
    if verbose: print('Taking inventory')
    current = take_inventory(config)
    local = read_inventory('.dat/local')

    # Create push, purg lists
    if verbose: print('Creating push, purge lists')
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    # Either exit or get master
    if len(push | purg) == 0:
        exit('Everything up-to-date')
    else:
        if verbose: print('Obtaining master')
        master = get_master(config, local)

    # Check for conflicts
    if verbose: print('Checking for conflicts')
    [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push)
    [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg)
    conflict = sorted(push_conflict | purg_conflict)
    if len(conflict) > 0:
        print(red("Unable to push the following files: conflict with master\n" + '\n'.join(conflict)))

    # Sync
    if verbose: print('Pushing')
    resolved = sorted(push_resolved | purg_resolved)
    if len(push | purg):
        opt = '--delete --exclude "*" --include .dat/master'
        for f in sorted((push | purg) - push_conflict - purg_conflict - push_resolved - purg_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        if 'profile' in config.keys():
            opt = opt + f" --profile {config['profile']}"
        cmd = f"aws s3 sync --no-follow-symlinks . s3://{config['aws']} {opt} --region {config['region']}"
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

def dat_pull(dry=False, verbose=False, region='us-east-1'):
    # Read in config file
    if verbose: print('Reading config')
    config = read_config()

    # Set the region
    config['region'] = region if region else config.get('region', 'us-east-1')

    # Get master/current/local
    if verbose: print('Taking inventory')
    current = take_inventory(config)
    local = read_inventory('.dat/local')
    if verbose: print('Obtaining master')
    master = get_master(config)

    # Create pull, purge lists
    if verbose: print('Creating pull, purge lists')
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)

    # Check for conflicts
    if verbose: print('Checking for conflicts')
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = sorted(pull_conflict | kill_conflict)
    if len(conflict) > 0:
        print(red("Unable to pull the following files: conflict with current\n  " + '\n  '.join(conflict)))

    # Sync
    if verbose: print('Pulling')
    resolved = sorted(kill_resolved | pull_resolved)
    if len(pull | kill):
        opt = '--delete --exclude "*"'
        for f in sorted((pull | kill) - pull_conflict - kill_conflict - pull_resolved - kill_resolved):
            opt = opt + ' --include ' + '"' + re.sub('^_site', '', f).lstrip('/') + '"'
        cmd = f"aws s3 sync s3://{config['aws']} . {opt} --region {config['region']}"
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

def dat_repair_master():
    # download master
    config = read_config()
    if os.path.isdir('.dat/remote'):
        quit(red('.dat/remote: This directory already exists. repair-master cannot continue'))
    cmd = f"aws s3 cp s3://{config['aws']} .dat/remote --recursive"
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    try:
        os.system(cmd)
    except:
        quit(red('Failed to download remote; are you logged in?'))

    # take inventory
    os.chdir('.dat/remote')
    master = take_inventory(config)
    write_inventory(master, '.dat/master')
    os.chdir('../../')

    # upload master
    cmd = f"aws s3 sync --no-follow-symlinks --delete .dat/remote s3://{config['aws']}/"
    if 'profile' in config.keys():
        cmd = cmd + f" --profile {config['profile']}"
    try:
        os.system(cmd)
    except:
        quit(red('Failed to upload master; are you logged in?'))

    # clean up
    shutil.rmtree('.dat/remote')

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

import boto3
import json
from botocore.exceptions import ClientError

def dat_share(account_number, username=None, root=False, verbose=False):
    """
    Shares the S3 bucket with another AWS account or IAM user.

    Parameters:
        account_number (str): The AWS account number to share the bucket with.
        username (str, optional): The IAM username within the account. Required if root is False.
        root (bool, optional): Whether to share with the root account. Defaults to False.
        verbose (bool, optional): Enable verbose output for debugging. Defaults to False.
    """
    # Read the bucket name from .dat/config
    config = read_config()
    if 'aws' not in config:
        raise ValueError("Bucket name not found in .dat/config.")
    bucket_name = config['aws'].split('/')[0]

    if verbose:
        print(f"[DEBUG] Bucket name extracted from config: {bucket_name}")

    # Set up the AWS S3 client
    s3 = boto3.client('s3')

    # Construct the ARN
    if root:
        user_arn = f"arn:aws:iam::{account_number}:root"
    else:
        if not username:
            raise ValueError("Username is required unless specifying --root.")
        user_arn = f"arn:aws:iam::{account_number}:user/{username}"

    if verbose:
        print(f"[DEBUG] Using ARN: {user_arn}")

    # Construct the policy statements
    statements = [
        {
            "Effect": "Allow",
            "Principal": {"AWS": user_arn},
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*"
            ]
        }
    ]

    if verbose:
        print(f"[DEBUG] Constructed policy statements: {json.dumps(statements, indent=2)}")

    # Attempt to retrieve existing bucket policy
    try:
        response = s3.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(response['Policy'])
        if verbose:
            print(f"[DEBUG] Existing bucket policy retrieved:\n{json.dumps(policy, indent=2)}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucketPolicy':
            if verbose:
                print(f"[DEBUG] No existing bucket policy found. Creating a new one.")
            policy = {
                "Version": "2012-10-17",
                "Statement": []
            }
        else:
            raise e

    # Check for existing statements with the same principal
    existing_principals = {
        statement['Principal']['AWS'] for statement in policy['Statement']
        if 'Principal' in statement and 'AWS' in statement['Principal']
    }

    if user_arn in existing_principals:
        print(f"Access already granted to {user_arn} for bucket '{bucket_name}'.")
        if verbose:
            print(f"[DEBUG] No changes made to the bucket policy.")
        return

    # Append new statements to the policy
    policy['Statement'].extend(statements)

    if verbose:
        print(f"[DEBUG] Updated bucket policy to be applied:\n{json.dumps(policy, indent=2)}")

    # Apply the updated policy
    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        print(f"Access successfully granted to {user_arn} for bucket '{bucket_name}'.")
        if verbose:
            print(f"[DEBUG] Bucket policy updated successfully.")
    except ClientError as e:
        print(f"Error applying bucket policy: {e.response['Error']['Message']}")
        if verbose:
            print(f"[DEBUG] Failed to update bucket policy due to error code: {e.response['Error']['Code']}")


def read_config(filename='.dat/config'):
    if not os.path.isfile(filename):
        sys.exit(red(f'Not a dat repository; {filename} does not exist'))

    config = {}
    with open(filename, 'r') as f:
        for line in f:
            key, value = [x.strip() for x in line.split(':', 1)]
            config[key] = value
    return config
