#!/usr/bin/env python3
"""Push/pull system for cloud synchronization of large files via S3."""

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
import subprocess
import textwrap
import fnmatch
from pathlib import Path
from botocore.exceptions import ClientError, CredentialRetrievalError, NoCredentialsError
import argparse

try:
    import xxhash as _xxhash
    _XXHASH_AVAILABLE = True
except ImportError:
    _XXHASH_AVAILABLE = False

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
    parser = argparse.ArgumentParser(
        prog="dat",
        description="Push/pull system for cloud synchronization of large files via S3.",
    )
    sub = parser.add_subparsers(dest="command", metavar="command", required=True)

    p = sub.add_parser("init", help="initialize a new dat repository")
    p.add_argument("bucket", nargs="?", help="S3 bucket name (auto-generated if omitted)")
    p.add_argument("--profile", metavar="PROFILE", help="AWS CLI profile")

    p = sub.add_parser("checkin", help="check in a single file")
    p.add_argument("file")

    p = sub.add_parser("checkout", help="check out a single file")
    p.add_argument("file")

    p = sub.add_parser("clone", help="clone a remote dat repository")
    p.add_argument("bucket", help="S3 bucket name or bucket/prefix")
    p.add_argument("folder", nargs="?", help="local folder name (defaults to bucket name)")
    p.add_argument("--profile", metavar="PROFILE", help="AWS CLI profile")

    sub.add_parser("delete", help="delete remote bucket and local .dat directory")

    p = sub.add_parser("pull", help="pull remote changes")
    p.add_argument("-d", action="store_true", help="dry run")
    p.add_argument("-v", action="store_true", help="verbose")

    p = sub.add_parser("push", help="push local changes")
    p.add_argument("-d", action="store_true", help="dry run")
    p.add_argument("-v", action="store_true", help="verbose")

    p = sub.add_parser("stash", help="stash conflicted files")
    stash_sub = p.add_subparsers(dest="stash_command", metavar="subcommand")
    p2 = stash_sub.add_parser("pop", help="restore stashed files")
    p2.add_argument("--hard", action="store_true", help="overwrite existing files")

    p = sub.add_parser("status", help="show sync status")
    p.add_argument("-r", action="store_true", help="check against remote")

    sub.add_parser("overwrite-master", help="overwrite remote with local copy")
    sub.add_parser("repair-master", help="repair corrupted remote master inventory")

    p = sub.add_parser("rehash", help="convert repository to a different hash algorithm")
    p.add_argument(
        "algo",
        nargs="?",
        default="xxh3_64",
        choices=["md5", "xxhash", "xxh3_64"],
        help="hash algorithm (default: xxh3_64)",
    )
    p.add_argument("-d", action="store_true", help="dry run")

    p = sub.add_parser("share", help="share bucket with another AWS account")
    p.add_argument("account_number", help="AWS account number")
    p.add_argument("username", nargs="?", help="IAM username (omit with --root)")
    p.add_argument("--root", action="store_true", help="share with root account")
    p.add_argument("-v", action="store_true", help="verbose")

    arg = parser.parse_args()

    if arg.command == "init":
        dat_init(arg.bucket, arg.profile)
    elif arg.command == "checkin":
        dat_checkin(arg.file)
    elif arg.command == "checkout":
        dat_checkout(arg.file)
    elif arg.command == "clone":
        dat_clone(arg.bucket, arg.folder, arg.profile)
    elif arg.command == "delete":
        dat_delete()
    elif arg.command == "push":
        dat_push(arg.d, arg.v)
    elif arg.command == "pull":
        dat_pull(arg.d, arg.v)
    elif arg.command == "stash":
        if arg.stash_command == "pop":
            dat_pop(arg.hard)
        else:
            dat_stash()
    elif arg.command == "status":
        dat_status(arg.r)
    elif arg.command == "overwrite-master":
        dat_overwrite_master()
    elif arg.command == "repair-master":
        dat_repair_master()
    elif arg.command == "rehash":
        dat_rehash(arg.algo, arg.d)
    elif arg.command == "share":
        dat_share(arg.account_number, arg.username, root=arg.root, verbose=arg.v)


# ---------------------------------------------------------------------------
# ANSI color helpers
# ---------------------------------------------------------------------------

def red(x):
    return "\033[01;38;5;196m" + x + "\033[0m"


def green(x):
    return "\033[01;38;5;46m" + x + "\033[0m"


def blue(x):
    return "\033[01;38;5;39m" + x + "\033[0m"


def die(msg):
    sys.exit(red(msg))


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def _hash_md5(fname):
    h = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_xxh3_64(fname):
    if not _XXHASH_AVAILABLE:
        die("xxhash is not installed; run: pip install xxhash")
    h = _xxhash.xxh3_64()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_file(fname, algorithm):
    if algorithm == "md5":
        return _hash_md5(fname)
    elif algorithm == "xxh3_64":
        return _hash_xxh3_64(fname)
    else:
        die(f"Unknown hash algorithm: {algorithm}")


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
        config_repair(self.config, config_path)
        self._s3 = None
        self.bucket, self.prefix = _parse_bucket(self.config["aws"])

    @property
    def s3(self):
        if self._s3 is None:
            try:
                if "profile" in self.config:
                    session = boto3.Session(profile_name=self.config["profile"])
                    self._s3 = session.client("s3")
                else:
                    self._s3 = boto3.client("s3")
            except (NoCredentialsError, CredentialRetrievalError) as e:
                die(f"AWS credentials unavailable\n\n{e}\nRun 'aws login' or configure credentials first.")
        return self._s3

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
            self.master_hash = read_inventory_hash(_MASTER)
            master = read_inventory(_MASTER)
            _MASTER.unlink()
            if self.master_hash is None and master:
                sample = next(iter(master.values()))
                self.master_hash = "md5" if len(sample) == 32 else "xxh3_64"
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey", "NoSuchBucket"):
                if self.config.get("pushed") == "False":
                    if local is None:
                        die("Repository has never been pushed; run 'dat push' first")
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
                    self.master_hash = None
                    master = local.copy()
                else:
                    die("Bucket exists (according to config) but cannot be accessed; are you logged in?")
            else:
                die(f"Failed to access S3: {e}")
        return master

    def save_config(self, filename=_CONFIG):
        write_config(self.config, filename)


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def _iter_files(root, symlinks="follow"):
    """Yield all files under root, skipping .dat and .git directories."""
    for item in root.iterdir():
        if symlinks == "ignore" and item.is_symlink():
            continue
        if item.is_dir():
            if item.name not in (".dat", ".git"):
                yield from _iter_files(item, symlinks=symlinks)
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


def _is_ignored(f, patterns):
    """Return True if f should be ignored given an ordered list of patterns.

    Patterns are evaluated in order; last match wins.  A bare name or path
    matches both itself and everything beneath it (e.g. 'data' also matches
    'data/file.txt').  Prefix a pattern with '!' to negate a prior match.
    """
    ignored = False
    for pattern in patterns:
        if pattern.startswith("!"):
            if fnmatch.fnmatch(f, pattern[1:]):
                ignored = False
        else:
            if fnmatch.fnmatch(f, pattern) or fnmatch.fnmatch(f, pattern + "/*"):
                ignored = True
    return ignored


def take_inventory(config, root=None):
    root = Path(root) if root is not None else Path(".")
    ignore_patterns = read_ignore_patterns(root / ".dat" / "ignore")
    symlinks = config.get("symlinks", "follow")
    out = {}
    for path in _iter_files(root, symlinks=symlinks):
        f = str(path.relative_to(root))
        if _is_ignored(f, ignore_patterns):
            continue
        out[f] = hash_file(path, config.get("hash", "md5"))
    return out


def write_inventory(x, fname, hash_algo):
    with open(fname, "w") as f:
        f.write(f"# hash: {hash_algo}\n")
        for d in sorted(x.keys()):
            f.write(d + "\t" + x[d] + "\n")


def read_inventory(fname=_LOCAL):
    fname = Path(fname)
    if fname.is_file():
        out = {}
        for line in fname.read_text().splitlines():
            if line and not line.startswith("#"):
                row = line.split("\t")
                out[row[0]] = row[1]
        return out
    return {}


def read_inventory_hash(fname):
    fname = Path(fname)
    if fname.is_file():
        for line in fname.read_text().splitlines():
            if line.startswith("# hash:"):
                return line[len("# hash:"):].strip()
    return None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def read_config(filename=_CONFIG):
    filename = Path(filename)
    if not filename.is_file():
        die(f"Not a dat repository; {filename} does not exist")

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


def _repo_has_symlinks(root=None):
    root = Path(".") if root is None else Path(root)
    for item in root.iterdir():
        if item.is_symlink():
            return True
        if item.is_dir() and item.name not in (".dat", ".git"):
            if _repo_has_symlinks(item):
                return True
    return False


def config_repair(config, config_path):
    """Add any keys missing from an older config, writing the file if changed."""
    changed = False
    if "hash" not in config:
        config["hash"] = "md5"
        changed = True
    if "symlinks" not in config:
        if _repo_has_symlinks():
            config["symlinks"] = "follow"
            print("Note: updated config (symlinks: follow — existing symlinks detected)")
        else:
            config["symlinks"] = "ignore"
            print("Note: updated config (symlinks: ignore)")
        changed = True
    if changed:
        write_config(config, config_path)


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
        die(f'"{filename}" does not exist')
    repo = DatRepo()

    current = take_inventory(repo.config)
    local = read_inventory()
    local[filename] = current[filename]
    master = repo.get_master(local)
    master[filename] = current[filename]

    try:
        write_inventory(master, _MASTER, repo.config["hash"])
        repo.upload(filename)
        repo.upload(_MASTER)
        write_inventory(local, _LOCAL, repo.config["hash"])
        _MASTER.unlink()
    except ClientError as e:
        die(f"Failed to push file: {e}")


def dat_checkout(filename):
    repo = DatRepo()

    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    try:
        repo.download(filename)
    except ClientError as e:
        die(f"Failed to pull file: {e}")

    current = take_inventory(repo.config)
    local = read_inventory()
    local[filename] = current[filename]
    write_inventory(local, _LOCAL, repo.config["hash"])


def dat_clone(bucket, folder, profile=None):
    if folder is None:
        folder = bucket

    folder_path = Path(folder)
    if folder_path.is_dir():
        die(f'Directory "{folder}" already exists')
    folder_path.mkdir()

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    try:
        session.client("sts").get_caller_identity()
    except ClientError:
        shutil.rmtree(folder_path, ignore_errors=True)
        die("You are not currently logged into AWS")

    s3 = session.client("s3")
    b, prefix = _parse_bucket(bucket)
    try:
        _download_all(s3, b, prefix, folder_path)
    except ClientError as e:
        shutil.rmtree(folder_path, ignore_errors=True)
        die(f"Failed to clone repository: {e}")

    remote_config_path = folder_path / ".dat" / "config"
    remote_config = {}
    if remote_config_path.is_file():
        for line in remote_config_path.read_text().splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                remote_config[k.strip()] = v.strip()
    hash_algo = remote_config.get("hash", "md5")

    config = {"aws": bucket, "hash": hash_algo, "pushed": "True", "symlinks": "ignore"}
    if profile is not None:
        config["profile"] = profile
    write_config(config, remote_config_path)

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
            die('Token has expired; run "aws login"')

        if repo.bucket in all_buckets:
            paginator = repo.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=repo.bucket):
                objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if objects:
                    repo.s3.delete_objects(Bucket=repo.bucket, Delete={"Objects": objects})
            repo.s3.delete_bucket(Bucket=repo.bucket)
            print(f"Deleted aws bucket: {repo.bucket}")
        else:
            die(f"Bucket {repo.bucket} does not exist")

    shutil.rmtree(_DAT_DIR)


def dat_init(id, profile):
    if _DAT_DIR.is_dir():
        die("Error: .dat directory already exists")
    _DAT_DIR.mkdir()

    if id is None:
        username = getpass.getuser()
        id = f"{username}.{str(Path.cwd()).replace(str(Path.home()), '').strip('/').replace('/', '.').lower()}"

    hash_algo = "xxh3_64" if _XXHASH_AVAILABLE else "md5"
    config = {"aws": id, "hash": hash_algo, "pushed": "False", "symlinks": "ignore"}
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
        die("Exiting...")

    current = take_inventory(repo.config)
    write_inventory(current, _MASTER, repo.config["hash"])
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

        write_inventory(current, _LOCAL, repo.config["hash"])
        _MASTER.unlink()
    except ClientError as e:
        die(f"Failed to overwrite remote: {e}")


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

    local_hash = repo.config.get("hash", "md5")
    if repo.master_hash and repo.master_hash != local_hash:
        die(
            f"Remote master uses '{repo.master_hash}' hashes but local config uses '{local_hash}'.\n"
            f"Run 'dat rehash {repo.master_hash}' to convert your local repo, then pull again."
        )

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
            write_inventory(local, _LOCAL, repo.config["hash"])
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
        write_inventory(local, _LOCAL, repo.config["hash"])


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
        if Path(f).exists() and _is_ignored(f, ignore_patterns)
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
            write_inventory(master, _MASTER, repo.config["hash"])
            repo.upload(_MASTER)
            write_inventory(local, _LOCAL, repo.config["hash"])
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
        write_inventory(master, _MASTER, repo.config["hash"])
        repo.upload(_MASTER)
        for f in sorted(active & push):
            repo.upload(f)
        for f in sorted(active & purg):
            try:
                repo.delete(f)
            except ClientError:
                pass
        write_inventory(local, _LOCAL, repo.config["hash"])
        _MASTER.unlink()
        repo.config["pushed"] = "True"
        repo.save_config()


def dat_rehash(algo="xxh3_64", dry=False):
    if algo == "xxhash":
        algo = "xxh3_64"

    repo = DatRepo()
    current_algo = repo.config.get("hash", "md5")

    if algo == current_algo:
        print(f"Already using {algo}; nothing to do.")
        return

    # Check for unpushed/unpurged local changes before anything else
    current = take_inventory(repo.config)
    local = read_inventory()
    push = needs_push(current, local)
    purge = needs_purge(current, local)
    if push or purge:
        items = sorted(push | purge)
        die("Local changes have not been pushed; run 'dat push' first:\n  " + "\n  ".join(items))

    # Check for remote changes
    master = repo.get_master()
    pull = needs_pull(master, local)
    kill = needs_kill(master, local)

    if dry:
        if pull or kill:
            print("Note: remote has unsynced changes; run 'dat pull' first before rehashing.")
        print(f"Would rehash {len(current)} files from {current_algo} to {algo} and update the remote master.")
        return

    if pull or kill:
        confirm = input("There are remote changes. Run 'dat pull' first (recommended)? [Y/n]: ").strip().lower()
        if confirm in ("", "y"):
            try:
                dat_pull()
            except SystemExit as e:
                if e.code != 0:
                    sys.exit(e.code)
            local = read_inventory()
            remaining = needs_pull(master, local) | needs_kill(master, local)
            if remaining:
                die("Pull did not complete (conflicts?). Resolve issues before rehashing.")
        else:
            die("Cannot rehash with unsynced remote changes. Run 'dat pull' first.")

    n = len(local)
    print(f"\nThis will rehash all {n} files using {algo} and update the remote master.")
    print("Collaborators will need to run 'dat rehash' before their next pull.")
    confirm = input("Proceed? [Y/n]: ").strip().lower()
    if confirm not in ("", "y"):
        die("Aborted.")

    # Rehash with new algorithm
    repo.config["hash"] = algo
    new_hashes = take_inventory(repo.config)

    missing = set(master.keys()) - set(new_hashes.keys())
    if missing:
        repo.config["hash"] = current_algo
        die("Cannot rehash: files in master are missing locally:\n  " + "\n  ".join(sorted(missing)))

    new_master = {f: new_hashes[f] for f in master}

    try:
        write_inventory(new_master, _MASTER, repo.config["hash"])
        repo.upload(_MASTER)
        write_config(repo.config, _CONFIG)
        write_inventory(new_hashes, _LOCAL, repo.config["hash"])
        _MASTER.unlink()
    except ClientError as e:
        die(f"Failed to upload new master: {e}")

    print(f"Done. Rehashed {n} files; remote master and local inventory updated.")


def dat_pop(hard=False):
    if not _STASH.is_dir():
        die("Error: No stash detected!")
    for item in _STASH.iterdir():
        if item.is_file():
            if hard:
                shutil.move(item, Path(".") / item.name)
            else:
                die(
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
        die(".dat/remote: This directory already exists. repair-master cannot continue")

    try:
        repo.download_all(_REMOTE)
    except ClientError as e:
        shutil.rmtree(_REMOTE, ignore_errors=True)
        die(f"Failed to download remote: {e}")

    remote_master = _REMOTE / ".dat" / "master"
    remote_master.parent.mkdir(parents=True, exist_ok=True)
    master = take_inventory(repo.config, root=_REMOTE)
    write_inventory(master, remote_master, repo.config["hash"])

    try:
        repo.upload(remote_master, ".dat/master")
    except ClientError as e:
        die(f"Failed to upload master: {e}")
    finally:
        shutil.rmtree(_REMOTE)


def dat_stash():
    repo = DatRepo()

    if _STASH.is_dir():
        die("Error: Unpopped stash detected!")

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
        write_inventory(local, _LOCAL, repo.config["hash"])


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
        write_inventory(local, _LOCAL, repo.config["hash"])
        local = olocal.copy()
        [purg_conflict, purg_resolved] = resolve_purge_conflicts(
            master, local, purg, hard=False
        )
        master = omaster.copy()
        write_inventory(local, _LOCAL, repo.config["hash"])
        local = olocal.copy()
        [pull_conflict, pull_resolved] = resolve_pull_conflicts(
            current, local, master, pull, hard=False
        )
        master = omaster.copy()
        write_inventory(local, _LOCAL, repo.config["hash"])
        local = olocal.copy()
        [kill_conflict, kill_resolved] = resolve_kill_conflicts(
            current, local, kill, hard=False
        )
        write_inventory(local, _LOCAL, repo.config["hash"])

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
            die("Username is required unless specifying --root.")
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
            die(f"S3 error getting bucket policy: {e}")

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
