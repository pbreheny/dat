#!/usr/bin/env python3
"""Push/pull system for cloud synchronization

Usage:
    dat init [--profile=<profile>] [<bucket>]
    dat checkin <file>
    dat checkout <file>
    dat clone [--profile=<profile>] <bucket> [<folder>]
    dat delete
    dat [-d] [-v] pull
    dat [-d] [-v] push
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
    --profile=<profile>      AWS CLI profile to use
    --hard                   Overwrite existing files when popping stash
    --root                   Share the bucket with the root account (omit <username> when using this)
"""

# Definitions:
#   push: local file is changed/new
#   pull: remote file is changed/new
#   purge: local file has been deleted (remove from master?)
#   kill: remote file has been deleted (remove from current?)

import os
import sys
import boto3
import json
import shutil
import hashlib
import platform
import subprocess
import textwrap
import fnmatch
from glob import glob
from botocore.exceptions import ClientError
from docopt import docopt


def dat():
    arg = docopt(__doc__)
    if arg["init"]:
        dat_init(arg["<bucket>"], arg["--profile"])
    elif arg["checkin"]:
        dat_checkin(arg["<file>"])
    elif arg["checkout"]:
        dat_checkout(arg["<file>"])
    elif arg["clone"]:
        dat_clone(arg["<bucket>"], arg["<folder>"], arg["--profile"])
    elif arg["delete"]:
        dat_delete()
    elif arg["push"]:
        dat_push(arg["-d"], arg["-v"])
    elif arg["pull"]:
        dat_pull(arg["-d"], arg["-v"])
    elif arg["stash"]:
        if arg["pop"]:
            dat_pop(arg["--hard"])
        else:
            dat_stash()
    elif arg["status"]:
        dat_status(arg["-r"])
    elif arg["overwrite-master"]:
        dat_overwrite_master()
    elif arg["repair-master"]:
        dat_repair_master()
    elif arg["share"]:
        dat_share(
            arg["<account_number>"],
            arg["<username>"],
            root=arg["--root"],
            verbose=arg["-v"],
        )


# ANSI escape sequences
def red(x):
    return "\033[01;38;5;196m" + x + "\033[0m"


def green(x):
    return "\033[01;38;5;46m" + x + "\033[0m"


def blue(x):
    return "\033[01;38;5;39m" + x + "\033[0m"


def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_client(config):
    if "profile" in config:
        return boto3.Session(profile_name=config["profile"]).client("s3")
    return boto3.client("s3")


def _parse_bucket(aws_str):
    """Return (bucket, prefix) where prefix is an empty string when absent."""
    parts = aws_str.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
    return bucket, prefix


def _full_key(prefix, path):
    """Build an S3 key from an optional prefix and a relative path."""
    return f"{prefix}/{path}" if prefix else path


def _download_all(s3, bucket, prefix, dest_dir):
    """Download every object under prefix into dest_dir, preserving relative paths."""
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix + "/"
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix) + 1:] if prefix else key
            if not rel:
                continue
            local_path = os.path.join(dest_dir, rel)
            os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
            s3.download_file(bucket, key, local_path)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def read_ignore_patterns():
    ignore_patterns = []
    ignore_file = ".dat/ignore"
    if os.path.isfile(ignore_file):
        with open(ignore_file, "r") as f:
            for line in f:
                pattern = line.strip()
                if pattern and not pattern.startswith("#"):
                    ignore_patterns.append(pattern)
    return ignore_patterns


def take_inventory(config):
    ignore_patterns = read_ignore_patterns()
    inv = []
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in [".dat", ".git"]]
        for file in files:
            file_path = os.path.relpath(os.path.join(root, file), ".")
            if file_path.startswith(".dat") or file_path.startswith(".git"):
                continue
            if any(fnmatch.fnmatch(file_path, pattern) for pattern in ignore_patterns):
                continue
            inv.append(file_path)
    out = dict()
    for f in inv:
        out[f] = md5(f)
    return out


def write_inventory(x, fname):
    f = open(fname, "w")
    for d in sorted(x.keys()):
        f.write(d + "\t" + x[d] + "\n")
    f.close()


def read_inventory(fname=".dat/local"):
    if os.path.isfile(fname):
        f = open(fname)
        out = dict()
        for line in f:
            row = line.strip().split("\t")
            out[row[0]] = row[1]
    else:
        out = {}
    return out


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def read_config(filename=".dat/config"):
    if not os.path.isfile(filename):
        sys.exit(red(f"Not a dat repository; {filename} does not exit"))

    if os.path.isfile(".dat/local"):
        if git_tracked():
            x = (
                subprocess.run(
                    ["git", "check-ignore", ".dat/local"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                .stdout.decode()
                .strip()
            )
            if x != ".dat/local":
                terminal_width = shutil.get_terminal_size().columns
                msg = (
                    "Warning! You appear to be tracking .dat/local with git. "
                    "This will almost certainly prevent dat from working correctly. Add"
                )
                print(
                    red(
                        textwrap.fill(msg, width=terminal_width)
                        + "\n**/.dat/local\nto your .gitignore file"
                    )
                )

    config = {}
    for line in open(filename):
        y = [x.strip() for x in line.split(":")]
        config[y[0]] = y[1]
    return config


def write_config(config, filename=".dat/config"):
    config_file = open(filename, "w")
    for k in sorted(config.keys()):
        config_file.write(f"{k}: {config[k]}\n")
    config_file.close()


def get_aws_region(profile=None):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.region_name


def get_master(config, local=None):
    if "aws" not in config:
        sys.exit(red("Only aws pulls are supported in this version"))

    s3 = _s3_client(config)
    bucket, prefix = _parse_bucket(config["aws"])

    try:
        s3.download_file(bucket, _full_key(prefix, ".dat/master"), ".dat/master")
        master = read_inventory(".dat/master")
        os.remove(".dat/master")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            if config.get("pushed") == "False":
                if local is None:
                    sys.exit(red("Repository has never been pushed; run 'dat push' first"))
                region = get_aws_region(config.get("profile"))
                if "/" in config["aws"]:
                    b, path_parts = config["aws"].split("/", 1)
                    s3.put_object(Bucket=b, Key=f"{path_parts.rstrip('/')}/")
                elif region:
                    s3.create_bucket(
                        Bucket=bucket,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )
                else:
                    s3.create_bucket(Bucket=bucket)
                master = local.copy()
            else:
                sys.exit(
                    red(
                        "Bucket exists (according to config) but cannot be accessed; are you logged in?"
                    )
                )
        else:
            sys.exit(red(f"Failed to access S3: {e}"))

    return master


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Conflict resolution
# ---------------------------------------------------------------------------

def resolve_push_conflicts(current, local, master, push, hard=True):
    conflict = set()
    resolved = set()
    for f in push:
        if f in local.keys():
            if f in master.keys():
                if master[f] == local[f]:
                    if hard:
                        master[f] = current[f]
                        local[f] = current[f]
                elif master[f] == current[f]:
                    local[f] = current[f]
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                master[f] = current[f]
                local[f] = current[f]
        else:
            if f in master.keys():
                if master[f] == current[f]:
                    local[f] = current[f]
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                master[f] = current[f]
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
                master.pop(f)
                local.pop(f)
        else:
            local.pop(f)
            resolved.add(f)
    return [conflict, resolved]


def resolve_pull_conflicts(current, local, master, pull, hard=True):
    conflict = set()
    resolved = set()
    for f in pull:
        if f in local.keys():
            if f in current.keys():
                if current[f] == local[f]:
                    if hard:
                        local[f] = master[f]
                elif current[f] == master[f]:
                    local[f] = master[f]
                    resolved.add(f)
                else:
                    conflict.add(f)
            else:
                conflict.add(f)
        else:
            if f in current.keys():
                if current[f] == master[f]:
                    local[f] = master[f]
                    resolved.add(f)
                else:
                    conflict.add(f)
            elif hard:
                local[f] = master[f]
    return [conflict, resolved]


def resolve_kill_conflicts(current, local, kill, hard=True):
    conflict = set()
    resolved = set()
    for f in kill:
        if f in current.keys():
            if current[f] != local[f]:
                conflict.add(f)
            elif hard:
                local.pop(f)
        else:
            local.pop(f)
            resolved.add(f)
    return [conflict, resolved]


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def dat_checkin(filename):
    if not os.path.isfile(filename):
        sys.exit(red(f'"{filename}" does not exist'))
    config = read_config()

    current = take_inventory(config)
    local = read_inventory(".dat/local")
    local[filename] = current[filename]
    master = get_master(config, local)
    master[filename] = current[filename]

    s3 = _s3_client(config)
    bucket, prefix = _parse_bucket(config["aws"])
    try:
        write_inventory(master, ".dat/master")
        s3.upload_file(filename, bucket, _full_key(prefix, filename))
        s3.upload_file(".dat/master", bucket, _full_key(prefix, ".dat/master"))
        write_inventory(local, ".dat/local")
        os.remove(".dat/master")
    except ClientError as e:
        sys.exit(red(f"Failed to push file: {e}"))


def dat_checkout(filename):
    config = read_config()

    s3 = _s3_client(config)
    bucket, prefix = _parse_bucket(config["aws"])
    dest_dir = os.path.dirname(filename)
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)
    try:
        s3.download_file(bucket, _full_key(prefix, filename), filename)
    except ClientError as e:
        sys.exit(red(f"Failed to pull file: {e}"))

    current = take_inventory(config)
    local = read_inventory(".dat/local")
    local[filename] = current[filename]
    write_inventory(local, ".dat/local")


def dat_clone(bucket, folder, profile=None):
    if folder is None:
        folder = bucket
    if ":" not in bucket:
        loc = "aws"
        id = bucket
    else:
        [loc, id] = bucket.split(":")

    if os.path.isdir(folder):
        sys.exit(red(f'Error: Directory "{folder}" already exists'))
    os.mkdir(folder)

    err = 0
    if loc == "aws":
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        try:
            session.client("sts").get_caller_identity()
        except ClientError:
            err = 1
            print(red("You are not currently logged into AWS"))

        if not err:
            s3 = session.client("s3")
            b, prefix = _parse_bucket(id)
            try:
                _download_all(s3, b, prefix, folder)
            except ClientError as e:
                err = 1
                print(red(f"Failed to clone repository: {e}"))
    elif loc == "hpc":
        if "argon" in platform.node():
            hub = "/Shared/Fisher/hub/"
        elif os.path.isdir(os.environ["HOME"] + "/lss"):
            hub = os.environ["HOME"] + "/lss/Fisher/hub/"
        else:
            hub = "hpc-data:/Shared/Fisher/hub/"
        err = os.system("rsync -avz " + hub + id + "/ " + folder + "/")
    else:
        err = 1
        print("Error: Central location must be of form aws:id or hpc:id")

    if err:
        shutil.rmtree(folder, ignore_errors=True)
        sys.exit(1)

    config = {"pushed": "True"}
    config[loc] = id
    if profile is not None:
        config["profile"] = profile
    write_config(config, f"{folder}/.dat/config")

    if os.path.isfile(folder + "/.dat/master"):
        os.rename(folder + "/.dat/master", folder + "/.dat/local")
    else:
        print("Warning: No .dat/master file -- upgrade dat version to md5")


def dat_delete():
    config = read_config()

    if "/" not in config["aws"]:
        s3 = _s3_client(config)
        bucket = config["aws"]
        try:
            all_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        except ClientError:
            sys.exit(red('Token has expired; run "aws login"'))

        if bucket in all_buckets:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            s3.delete_bucket(Bucket=bucket)
            print(f"Deleted aws bucket: {bucket}")
        else:
            sys.exit(red(f"Bucket {bucket} does not exist"))

    shutil.rmtree(".dat")


def dat_init(id, profile):
    if os.path.isdir(".dat"):
        sys.exit(red("Error: .dat directory already exists"))
    else:
        os.mkdir(".dat")

    if id is None:
        username = os.environ.get("USERNAME") or os.environ.get("USER")
        id = f"{username}.{os.getcwd().replace(os.environ['HOME'], '').strip('/').replace('/', '.').lower()}"

    config = open(".dat/config", "w")
    config.write(f"aws: {id}\n")
    config.write(f"pushed: False\n")
    if profile is not None:
        config.write(f"profile: {profile}\n")
        print(green(f"Configured for profile={profile} aws bucket: ") + id)
    else:
        print(green("Configured for aws bucket: ") + id)
    config.close()

    with open(".dat/ignore", "w") as ignore_file:
        ignore_file.write(".DS_Store\n")


def dat_overwrite_master():
    config = read_config()
    terminal_width = shutil.get_terminal_size().columns
    msg = "Warning: This will completely replace the remote dat repository with your local copy. Are you sure you want to do this?"
    confirm = input(
        textwrap.fill(msg, width=terminal_width) + "\nPress (y) to confirm: "
    )
    if confirm != "y":
        sys.exit("Exiting...")

    current = take_inventory(config)
    write_inventory(current, ".dat/master")
    if os.path.isfile(".dat/local"):
        os.remove(".dat/local")

    s3 = _s3_client(config)
    bucket, prefix = _parse_bucket(config["aws"])
    try:
        for f in current:
            s3.upload_file(f, bucket, _full_key(prefix, f))
        s3.upload_file(".dat/master", bucket, _full_key(prefix, ".dat/master"))

        # Delete S3 objects not present locally
        local_keys = {_full_key(prefix, f) for f in current}
        local_keys.add(_full_key(prefix, ".dat/master"))
        paginator = s3.get_paginator("list_objects_v2")
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix + "/"
        for page in paginator.paginate(**kwargs):
            to_delete = [
                {"Key": obj["Key"]}
                for obj in page.get("Contents", [])
                if obj["Key"] not in local_keys
            ]
            if to_delete:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})

        write_inventory(current, ".dat/local")
        os.remove(".dat/master")
    except ClientError as e:
        sys.exit(red(f"Failed to overwrite remote: {e}"))


def dat_pull(dry=False, verbose=False):
    if verbose:
        print("Reading config")
    config = read_config()

    if verbose:
        print("Taking inventory")
    current = take_inventory(config)
    local = read_inventory(".dat/local")
    if verbose:
        print("Obtaining master")
    master = get_master(config)

    if verbose:
        print("Creating pull, kill lists")
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)

    if verbose:
        print("Checking for conflicts")
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = sorted(pull_conflict | kill_conflict)
    if conflict:
        print(
            red(
                "Unable to pull the following files: conflict with current\n  "
                + "\n  ".join(conflict)
            )
        )

    resolved = sorted(kill_resolved | pull_resolved)
    active = (pull | kill) - pull_conflict - kill_conflict - pull_resolved - kill_resolved

    if not active and not conflict:
        if not dry:
            write_inventory(local, ".dat/local")
        print("Everything up-to-date")
        sys.exit(0)

    if verbose:
        print("Pulling")
    if dry:
        to_download = sorted(active & pull)
        to_remove = sorted(active & kill)
        if to_download:
            print("Would download: " + ", ".join(to_download))
        if to_remove:
            print("Would remove locally: " + ", ".join(to_remove))
        if resolved:
            print("Resolved: " + str(resolved))
    else:
        s3 = _s3_client(config)
        bucket, prefix = _parse_bucket(config["aws"])
        for f in sorted(active):
            if f in pull:
                dest_dir = os.path.dirname(f)
                if dest_dir:
                    os.makedirs(dest_dir, exist_ok=True)
                s3.download_file(bucket, _full_key(prefix, f), f)
            elif f in kill:
                if os.path.exists(f):
                    os.remove(f)
        write_inventory(local, ".dat/local")


def dat_push(dry=False, verbose=False):
    if verbose:
        print("Reading config")
    config = read_config()

    if verbose:
        print("Taking inventory")
    current = take_inventory(config)
    local = read_inventory(".dat/local")

    if verbose:
        print("Creating push, purge lists")
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if not push and not purg:
        print("Everything up-to-date")
        sys.exit(0)

    if verbose:
        print("Obtaining master")
    master = get_master(config, local)

    if verbose:
        print("Checking for conflicts")
    [push_conflict, push_resolved] = resolve_push_conflicts(current, local, master, push)
    [purg_conflict, purg_resolved] = resolve_purge_conflicts(master, local, purg)
    conflict = sorted(push_conflict | purg_conflict)
    if conflict:
        print(
            red(
                "Unable to push the following files: conflict with master\n"
                + "\n".join(conflict)
            )
        )

    # Delete ignored files that are still physically present from S3
    ignore_patterns = read_ignore_patterns()
    purge_ignored = {
        f for f in purg
        if os.path.exists(f) and any(fnmatch.fnmatch(f, pat) for pat in ignore_patterns)
    }
    if purge_ignored and not dry:
        s3 = _s3_client(config)
        bucket, prefix = _parse_bucket(config["aws"])
        for f in purge_ignored:
            if verbose:
                print(f"Removing ignored file {f} from S3...")
            try:
                s3.delete_object(Bucket=bucket, Key=_full_key(prefix, f))
            except ClientError:
                pass
    for f in purge_ignored:
        master.pop(f, None)
        local.pop(f, None)
    purg -= purge_ignored

    active = (push | purg) - push_conflict - purg_conflict - push_resolved - purg_resolved

    if verbose:
        print("Pushing")

    if not active:
        # Nothing to transfer; just update master
        if dry:
            print("[Dry Run] Would have synced updated .dat/master")
        else:
            s3 = _s3_client(config)
            bucket, prefix = _parse_bucket(config["aws"])
            write_inventory(master, ".dat/master")
            s3.upload_file(".dat/master", bucket, _full_key(prefix, ".dat/master"))
            write_inventory(local, ".dat/local")
            os.remove(".dat/master")
            config["pushed"] = "True"
            write_config(config)
        print("Master updated remotely, no other changes")
        sys.exit(0)

    resolved = sorted(push_resolved | purg_resolved)
    if dry:
        to_upload = sorted(active & push)
        to_delete = sorted(active & purg)
        if to_upload:
            print("Would upload: " + ", ".join(to_upload))
        if to_delete:
            print("Would delete from S3: " + ", ".join(to_delete))
        if resolved:
            print("Resolved: " + str(resolved))
    else:
        s3 = _s3_client(config)
        bucket, prefix = _parse_bucket(config["aws"])
        write_inventory(master, ".dat/master")
        s3.upload_file(".dat/master", bucket, _full_key(prefix, ".dat/master"))
        for f in sorted(active & push):
            s3.upload_file(f, bucket, _full_key(prefix, f))
        for f in sorted(active & purg):
            try:
                s3.delete_object(Bucket=bucket, Key=_full_key(prefix, f))
            except ClientError:
                pass
        write_inventory(local, ".dat/local")
        os.remove(".dat/master")
        config["pushed"] = "True"
        write_config(config)


def dat_pop(hard=False):
    if not os.path.isdir(".dat/stash"):
        sys.exit("Error: No stash detected!")
    for f in glob(r".dat/stash/*"):
        ff = os.path.basename(f)
        if os.path.isfile(f):
            if hard:
                shutil.move(f, "./" + ff)
            else:
                sys.exit(
                    f"Popping stash would overwrite file {ff}.\n"
                    "If you wish to overwrite existing files, rerun with\n"
                    "dat stash pop --hard"
                )
        else:
            shutil.move(f, ".")
    os.rmdir(".dat/stash")
    return ()


def dat_repair_master():
    config = read_config()
    if os.path.isdir(".dat/remote"):
        sys.exit(
            red(".dat/remote: This directory already exists. repair-master cannot continue")
        )

    s3 = _s3_client(config)
    bucket, prefix = _parse_bucket(config["aws"])

    try:
        _download_all(s3, bucket, prefix, ".dat/remote")
    except ClientError as e:
        shutil.rmtree(".dat/remote", ignore_errors=True)
        sys.exit(red(f"Failed to download remote: {e}"))

    old_cwd = os.getcwd()
    os.chdir(".dat/remote")
    master = take_inventory(config)
    write_inventory(master, ".dat/master")
    os.chdir(old_cwd)

    try:
        s3.upload_file(
            ".dat/remote/.dat/master", bucket, _full_key(prefix, ".dat/master")
        )
    except ClientError as e:
        sys.exit(red(f"Failed to upload master: {e}"))
    finally:
        shutil.rmtree(".dat/remote")


def dat_stash():
    config = read_config()

    if os.path.isdir(".dat/stash"):
        sys.exit("Error: Unpopped stash detected!")

    current = take_inventory(config)
    local = read_inventory(".dat/local")
    master = get_master(config)
    if len(local) == 0:
        local = current

    pull = needs_pull(master, local)
    kill = needs_kill(master, local)
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = pull_conflict.union(kill_conflict)

    os.mkdir(".dat/stash")
    for f in conflict:
        shutil.move(f, ".dat/stash/")
        local.pop(f)
        write_inventory(local, ".dat/local")


def dat_status(remote):
    config = read_config()
    current = take_inventory(config)
    local = read_inventory(".dat/local")

    if config["pushed"] == "False":
        print(red("dat initialized, but never pushed"))

    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if remote:
        master = get_master(config)
        olocal = local.copy()
        omaster = master.copy()

        pull = needs_pull(master, local)
        kill = needs_kill(master, local)

        [push_conflict, push_resolved] = resolve_push_conflicts(
            current, local, master, push, hard=False
        )
        master = omaster.copy()
        write_inventory(local, ".dat/local")
        local = olocal.copy()
        [purg_conflict, purg_resolved] = resolve_purge_conflicts(
            master, local, purg, hard=False
        )
        master = omaster.copy()
        write_inventory(local, ".dat/local")
        local = olocal.copy()
        [pull_conflict, pull_resolved] = resolve_pull_conflicts(
            current, local, master, pull, hard=False
        )
        master = omaster.copy()
        write_inventory(local, ".dat/local")
        local = olocal.copy()
        [kill_conflict, kill_resolved] = resolve_kill_conflicts(
            current, local, kill, hard=False
        )
        write_inventory(local, ".dat/local")

        all_conflict = pull_conflict | push_conflict | purg_conflict | kill_conflict
        conflict = sorted(all_conflict - (kill_conflict & push))
        if conflict:
            print(
                red(
                    "Local/remote conflicts in the following files:\n  "
                    + "\n  ".join(conflict)
                )
            )

        a = sorted(pull - pull_conflict - pull_resolved)
        if a:
            print(blue("Modified remotely: \n  ") + "\n  ".join(a))
        b = sorted(push - push_conflict - kill_conflict - push_resolved)
        if b:
            print(blue("Modified locally: \n  ") + "\n  ".join(b))
        c = sorted(kill - kill_conflict - kill_resolved)
        if c:
            print(blue("Deleted remotely: \n  ") + "\n  ".join(c))
        d = sorted(purg - purg_conflict - purg_resolved)
        if d:
            print(blue("Deleted locally: \n  ") + "\n  ".join(d))
        e = sorted(kill_conflict & push)
        if e:
            print(
                blue(
                    "Deleted remotely but modified locally (can be pushed, but should it?): \n  "
                )
                + "\n  ".join(e)
            )
        if not (a or b or c or d or conflict):
            print(green("Local is current with remote"))
    else:
        if not local:
            if config["pushed"] == "True":
                print(red("Local dat empty; never been pulled?"))
        else:
            if push or purg:
                if push:
                    print(blue("Modified locally: \n  ") + "\n  ".join(sorted(push)))
                if purg:
                    print(blue("Deleted locally: \n  ") + "\n  ".join(sorted(purg)))
            else:
                print(green("Nothing to push; local is clean"))


def dat_share(account_number, username=None, root=False, verbose=False):
    """
    Shares the S3 bucket with another AWS account or IAM user.

    Parameters:
        account_number (str): The AWS account number to share the bucket with.
        username (str, optional): The IAM username within the account. Required if root is False.
        root (bool, optional): Whether to share with the root account. Defaults to False.
        verbose (bool, optional): Enable verbose output for debugging. Defaults to False.
    """
    config = read_config()
    if "aws" not in config:
        raise ValueError("Bucket name not found in .dat/config.")
    bucket_name = config["aws"].split("/")[0]

    if verbose:
        print(f"[DEBUG] Bucket name extracted from config: {bucket_name}")

    s3 = boto3.client("s3")

    if root:
        user_arn = f"arn:aws:iam::{account_number}:root"
    else:
        if not username:
            raise ValueError("Username is required unless specifying --root.")
        user_arn = f"arn:aws:iam::{account_number}:user/{username}"

    if verbose:
        print(f"[DEBUG] Using ARN: {user_arn}")

    statements = [
        {
            "Effect": "Allow",
            "Principal": {"AWS": user_arn},
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket_name}",
                f"arn:aws:s3:::{bucket_name}/*",
            ],
        }
    ]

    if verbose:
        print(f"[DEBUG] Constructed policy statements: {json.dumps(statements, indent=2)}")

    try:
        response = s3.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(response["Policy"])
        if verbose:
            print(f"[DEBUG] Existing bucket policy retrieved:\n{json.dumps(policy, indent=2)}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucketPolicy":
            if verbose:
                print("[DEBUG] No existing bucket policy found. Creating a new one.")
            policy = {"Version": "2012-10-17", "Statement": []}
        else:
            raise e

    existing_principals = {
        statement["Principal"]["AWS"]
        for statement in policy["Statement"]
        if "Principal" in statement and "AWS" in statement["Principal"]
    }

    if user_arn in existing_principals:
        print(f"Access already granted to {user_arn} for bucket '{bucket_name}'.")
        if verbose:
            print("[DEBUG] No changes made to the bucket policy.")
        return

    policy["Statement"].extend(statements)

    if verbose:
        print(f"[DEBUG] Updated bucket policy to be applied:\n{json.dumps(policy, indent=2)}")

    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        print(f"Access successfully granted to {user_arn} for bucket '{bucket_name}'.")
        if verbose:
            print("[DEBUG] Bucket policy updated successfully.")
    except ClientError as e:
        print(f"Error applying bucket policy: {e.response['Error']['Message']}")
        if verbose:
            print(
                f"[DEBUG] Failed to update bucket policy due to error code: {e.response['Error']['Code']}"
            )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def git_tracked():
    try:
        subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False
