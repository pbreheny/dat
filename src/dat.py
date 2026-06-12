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

import sys
import boto3
import json
import shutil
import hashlib
import getpass
import platform
import subprocess
import textwrap
import fnmatch
from pathlib import Path
from botocore.exceptions import ClientError
from docopt import docopt

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

_DAT_DIR = Path(".dat")
_CONFIG  = _DAT_DIR / "config"
_LOCAL   = _DAT_DIR / "local"
_MASTER  = _DAT_DIR / "master"
_IGNORE  = _DAT_DIR / "ignore"
_STASH   = _DAT_DIR / "stash"
_REMOTE  = _DAT_DIR / "remote"


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


# ---------------------------------------------------------------------------
# ANSI color helpers
# ---------------------------------------------------------------------------

def red(x):
    return "\033[01;38;5;196m" + x + "\033[0m"


def green(x):
    return "\033[01;38;5;46m" + x + "\033[0m"


def blue(x):
    return "\033[01;38;5;39m" + x + "\033[0m"


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# ---------------------------------------------------------------------------
# Low-level S3 utilities (used by DatRepo and by dat_clone before config exists)
# ---------------------------------------------------------------------------

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
    dest_dir = Path(dest_dir)
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
            local_path = dest_dir / rel
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(local_path))


# ---------------------------------------------------------------------------
# DatRepo — owns config, S3 client, bucket, and prefix
# ---------------------------------------------------------------------------

class DatRepo:
    def __init__(self, config_path=_CONFIG):
        self.config = read_config(config_path)
        if "profile" in self.config:
            session = boto3.Session(profile_name=self.config["profile"])
            self.s3 = session.client("s3")
        else:
            self.s3 = boto3.client("s3")
        self.bucket, self.prefix = _parse_bucket(self.config["aws"])

    def key(self, path):
        return _full_key(self.prefix, str(path))

    def upload(self, local_path, s3_path=None):
        local_path = str(local_path)
        if s3_path is None:
            s3_path = local_path
        self.s3.upload_file(local_path, self.bucket, self.key(s3_path))

    def download(self, s3_path, local_path=None):
        if local_path is None:
            local_path = s3_path
        self.s3.download_file(self.bucket, self.key(str(s3_path)), str(local_path))

    def delete(self, s3_path):
        self.s3.delete_object(Bucket=self.bucket, Key=self.key(str(s3_path)))

    def download_all(self, dest_dir):
        _download_all(self.s3, self.bucket, self.prefix, dest_dir)

    def get_master(self, local=None):
        try:
            self.download(".dat/master", _MASTER)
            master = read_inventory(_MASTER)
            _MASTER.unlink()
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                if self.config.get("pushed") == "False":
                    if local is None:
                        sys.exit(red("Repository has never been pushed; run 'dat push' first"))
                    region = get_aws_region(self.config.get("profile"))
                    if "/" in self.config["aws"]:
                        b, path_parts = self.config["aws"].split("/", 1)
                        self.s3.put_object(Bucket=b, Key=f"{path_parts.rstrip('/')}/")
                    elif region:
                        self.s3.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={"LocationConstraint": region},
                        )
                    else:
                        self.s3.create_bucket(Bucket=self.bucket)
                    master = local.copy()
                else:
                    sys.exit(
                        red("Bucket exists (according to config) but cannot be accessed; are you logged in?")
                    )
            else:
                sys.exit(red(f"Failed to access S3: {e}"))
        return master

    def save_config(self, filename=_CONFIG):
        write_config(self.config, filename)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def _iter_files(root):
    """Yield all files under root, skipping .dat and .git directories."""
    for item in root.iterdir():
        if item.is_dir():
            if item.name not in (".dat", ".git"):
                yield from _iter_files(item)
        elif item.is_file():
            yield item


def read_ignore_patterns(ignore_file=_IGNORE):
    patterns = []
    ignore_file = Path(ignore_file)
    if ignore_file.is_file():
        for line in ignore_file.read_text().splitlines():
            pattern = line.strip()
            if pattern and not pattern.startswith("#"):
                patterns.append(pattern)
    return patterns


def take_inventory(config, root=None):
    root = Path(root) if root is not None else Path(".")
    ignore_patterns = read_ignore_patterns(root / ".dat" / "ignore")
    out = {}
    for path in _iter_files(root):
        f = str(path.relative_to(root))
        if any(fnmatch.fnmatch(f, pattern) for pattern in ignore_patterns):
            continue
        out[f] = md5(path)
    return out


def write_inventory(x, fname):
    with open(fname, "w") as f:
        for d in sorted(x.keys()):
            f.write(d + "\t" + x[d] + "\n")


def read_inventory(fname=_LOCAL):
    fname = Path(fname)
    if fname.is_file():
        out = {}
        for line in fname.read_text().splitlines():
            if line:
                row = line.split("\t")
                out[row[0]] = row[1]
        return out
    return {}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def read_config(filename=_CONFIG):
    filename = Path(filename)
    if not filename.is_file():
        sys.exit(red(f"Not a dat repository; {filename} does not exist"))

    if _LOCAL.is_file():
        if git_tracked():
            x = (
                subprocess.run(
                    ["git", "check-ignore", str(_LOCAL)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                )
                .stdout.decode()
                .strip()
            )
            if x != str(_LOCAL):
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
    for line in filename.read_text().splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            config[k.strip()] = v.strip()
    return config


def write_config(config, filename=_CONFIG):
    with open(filename, "w") as f:
        for k in sorted(config.keys()):
            f.write(f"{k}: {config[k]}\n")


def get_aws_region(profile=None):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    return session.region_name


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
    if not Path(filename).is_file():
        sys.exit(red(f'"{filename}" does not exist'))
    repo = DatRepo()

    current = take_inventory(repo.config)
    local = read_inventory()
    local[filename] = current[filename]
    master = repo.get_master(local)
    master[filename] = current[filename]

    try:
        write_inventory(master, _MASTER)
        repo.upload(filename)
        repo.upload(_MASTER)
        write_inventory(local, _LOCAL)
        _MASTER.unlink()
    except ClientError as e:
        sys.exit(red(f"Failed to push file: {e}"))


def dat_checkout(filename):
    repo = DatRepo()

    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    try:
        repo.download(filename)
    except ClientError as e:
        sys.exit(red(f"Failed to pull file: {e}"))

    current = take_inventory(repo.config)
    local = read_inventory()
    local[filename] = current[filename]
    write_inventory(local, _LOCAL)


def dat_clone(bucket, folder, profile=None):
    if folder is None:
        folder = bucket
    if ":" not in bucket:
        loc = "aws"
        id = bucket
    else:
        [loc, id] = bucket.split(":")

    folder_path = Path(folder)
    if folder_path.is_dir():
        sys.exit(red(f'Error: Directory "{folder}" already exists'))
    folder_path.mkdir()

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
                _download_all(s3, b, prefix, folder_path)
            except ClientError as e:
                err = 1
                print(red(f"Failed to clone repository: {e}"))
    elif loc == "hpc":
        if "argon" in platform.node():
            hub = "/Shared/Fisher/hub/"
        elif (Path.home() / "lss").is_dir():
            hub = str(Path.home() / "lss" / "Fisher" / "hub") + "/"
        else:
            hub = "hpc-data:/Shared/Fisher/hub/"
        result = subprocess.run(["rsync", "-avz", hub + id + "/", folder + "/"])
        err = result.returncode
    else:
        err = 1
        print("Error: Central location must be of form aws:id or hpc:id")

    if err:
        shutil.rmtree(folder_path, ignore_errors=True)
        sys.exit(1)

    config = {"pushed": "True"}
    config[loc] = id
    if profile is not None:
        config["profile"] = profile
    write_config(config, folder_path / ".dat" / "config")

    master_file = folder_path / ".dat" / "master"
    if master_file.is_file():
        master_file.rename(folder_path / ".dat" / "local")
    else:
        print("Warning: No .dat/master file -- upgrade dat version to md5")


def dat_delete():
    repo = DatRepo()

    if "/" not in repo.config["aws"]:
        try:
            all_buckets = [b["Name"] for b in repo.s3.list_buckets()["Buckets"]]
        except ClientError:
            sys.exit(red('Token has expired; run "aws login"'))

        if repo.bucket in all_buckets:
            paginator = repo.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=repo.bucket):
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    repo.s3.delete_objects(Bucket=repo.bucket, Delete={"Objects": objects})
            repo.s3.delete_bucket(Bucket=repo.bucket)
            print(f"Deleted aws bucket: {repo.bucket}")
        else:
            sys.exit(red(f"Bucket {repo.bucket} does not exist"))

    shutil.rmtree(_DAT_DIR)


def dat_init(id, profile):
    if _DAT_DIR.is_dir():
        sys.exit(red("Error: .dat directory already exists"))
    _DAT_DIR.mkdir()

    if id is None:
        username = getpass.getuser()
        id = f"{username}.{str(Path.cwd()).replace(str(Path.home()), '').strip('/').replace('/', '.').lower()}"

    config = {"aws": id, "pushed": "False"}
    if profile is not None:
        config["profile"] = profile
        print(green(f"Configured for profile={profile} aws bucket: ") + id)
    else:
        print(green("Configured for aws bucket: ") + id)
    write_config(config)

    _IGNORE.write_text(".DS_Store\n")


def dat_overwrite_master():
    repo = DatRepo()
    terminal_width = shutil.get_terminal_size().columns
    msg = "Warning: This will completely replace the remote dat repository with your local copy. Are you sure you want to do this?"
    confirm = input(
        textwrap.fill(msg, width=terminal_width) + "\nPress (y) to confirm: "
    )
    if confirm != "y":
        sys.exit("Exiting...")

    current = take_inventory(repo.config)
    write_inventory(current, _MASTER)
    if _LOCAL.is_file():
        _LOCAL.unlink()

    try:
        for f in current:
            repo.upload(f)
        repo.upload(_MASTER)

        # Delete S3 objects not present locally
        local_keys = {repo.key(f) for f in current}
        local_keys.add(repo.key(".dat/master"))
        paginator = repo.s3.get_paginator("list_objects_v2")
        kwargs = {"Bucket": repo.bucket}
        if repo.prefix:
            kwargs["Prefix"] = repo.prefix + "/"
        for page in paginator.paginate(**kwargs):
            to_delete = [
                {"Key": obj["Key"]}
                for obj in page.get("Contents", [])
                if obj["Key"] not in local_keys
            ]
            if to_delete:
                repo.s3.delete_objects(Bucket=repo.bucket, Delete={"Objects": to_delete})

        write_inventory(current, _LOCAL)
        _MASTER.unlink()
    except ClientError as e:
        sys.exit(red(f"Failed to overwrite remote: {e}"))


def dat_pull(dry=False, verbose=False):
    if verbose:
        print("Reading config")
    repo = DatRepo()

    if verbose:
        print("Taking inventory")
    current = take_inventory(repo.config)
    local = read_inventory()
    if verbose:
        print("Obtaining master")
    master = repo.get_master()

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
            write_inventory(local, _LOCAL)
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
        for f in sorted(active):
            if f in pull:
                Path(f).parent.mkdir(parents=True, exist_ok=True)
                repo.download(f)
            elif f in kill:
                p = Path(f)
                if p.exists():
                    p.unlink()
        write_inventory(local, _LOCAL)


def dat_push(dry=False, verbose=False):
    if verbose:
        print("Reading config")
    repo = DatRepo()

    if verbose:
        print("Taking inventory")
    current = take_inventory(repo.config)
    local = read_inventory()

    if verbose:
        print("Creating push, purge lists")
    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if not push and not purg:
        print("Everything up-to-date")
        sys.exit(0)

    if verbose:
        print("Obtaining master")
    master = repo.get_master(local)

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
        if Path(f).exists() and any(fnmatch.fnmatch(f, pat) for pat in ignore_patterns)
    }
    if purge_ignored and not dry:
        for f in purge_ignored:
            if verbose:
                print(f"Removing ignored file {f} from S3...")
            try:
                repo.delete(f)
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
            write_inventory(master, _MASTER)
            repo.upload(_MASTER)
            write_inventory(local, _LOCAL)
            _MASTER.unlink()
            repo.config["pushed"] = "True"
            repo.save_config()
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
        write_inventory(master, _MASTER)
        repo.upload(_MASTER)
        for f in sorted(active & push):
            repo.upload(f)
        for f in sorted(active & purg):
            try:
                repo.delete(f)
            except ClientError:
                pass
        write_inventory(local, _LOCAL)
        _MASTER.unlink()
        repo.config["pushed"] = "True"
        repo.save_config()


def dat_pop(hard=False):
    if not _STASH.is_dir():
        sys.exit("Error: No stash detected!")
    for item in _STASH.iterdir():
        if item.is_file():
            if hard:
                shutil.move(item, Path(".") / item.name)
            else:
                sys.exit(
                    f"Popping stash would overwrite file {item.name}.\n"
                    "If you wish to overwrite existing files, rerun with\n"
                    "dat stash pop --hard"
                )
        else:
            shutil.move(item, Path("."))
    _STASH.rmdir()


def dat_repair_master():
    repo = DatRepo()
    if _REMOTE.is_dir():
        sys.exit(
            red(".dat/remote: This directory already exists. repair-master cannot continue")
        )

    try:
        repo.download_all(_REMOTE)
    except ClientError as e:
        shutil.rmtree(_REMOTE, ignore_errors=True)
        sys.exit(red(f"Failed to download remote: {e}"))

    remote_master = _REMOTE / ".dat" / "master"
    remote_master.parent.mkdir(parents=True, exist_ok=True)
    master = take_inventory(repo.config, root=_REMOTE)
    write_inventory(master, remote_master)

    try:
        repo.upload(remote_master, ".dat/master")
    except ClientError as e:
        sys.exit(red(f"Failed to upload master: {e}"))
    finally:
        shutil.rmtree(_REMOTE)


def dat_stash():
    repo = DatRepo()

    if _STASH.is_dir():
        sys.exit("Error: Unpopped stash detected!")

    current = take_inventory(repo.config)
    local = read_inventory()
    master = repo.get_master()
    if not local:
        local = current

    pull = needs_pull(master, local)
    kill = needs_kill(master, local)
    [pull_conflict, pull_resolved] = resolve_pull_conflicts(current, local, master, pull)
    [kill_conflict, kill_resolved] = resolve_kill_conflicts(current, local, kill)
    conflict = pull_conflict.union(kill_conflict)

    _STASH.mkdir()
    for f in conflict:
        shutil.move(f, _STASH)
        local.pop(f)
        write_inventory(local, _LOCAL)


def dat_status(remote):
    repo = DatRepo()
    current = take_inventory(repo.config)
    local = read_inventory()

    if repo.config["pushed"] == "False":
        print(red("dat initialized, but never pushed"))

    push = needs_push(current, local)
    purg = needs_purge(current, local)

    if remote:
        master = repo.get_master()
        olocal = local.copy()
        omaster = master.copy()

        pull = needs_pull(master, local)
        kill = needs_kill(master, local)

        [push_conflict, push_resolved] = resolve_push_conflicts(
            current, local, master, push, hard=False
        )
        master = omaster.copy()
        write_inventory(local, _LOCAL)
        local = olocal.copy()
        [purg_conflict, purg_resolved] = resolve_purge_conflicts(
            master, local, purg, hard=False
        )
        master = omaster.copy()
        write_inventory(local, _LOCAL)
        local = olocal.copy()
        [pull_conflict, pull_resolved] = resolve_pull_conflicts(
            current, local, master, pull, hard=False
        )
        master = omaster.copy()
        write_inventory(local, _LOCAL)
        local = olocal.copy()
        [kill_conflict, kill_resolved] = resolve_kill_conflicts(
            current, local, kill, hard=False
        )
        write_inventory(local, _LOCAL)

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
            if repo.config["pushed"] == "True":
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
    repo = DatRepo()
    bucket_name = repo.bucket

    if verbose:
        print(f"[DEBUG] Bucket name extracted from config: {bucket_name}")

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
        response = repo.s3.get_bucket_policy(Bucket=bucket_name)
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
        repo.s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
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
