"""Tests for S3 integration via moto (no real AWS required)."""
import hashlib
import os
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Set dummy credentials before any boto3 call so the SDK never probes real AWS.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import dat as dat_module
from dat import (
    DatRepo,
    dat_checkin,
    dat_checkout,
    dat_clone,
    dat_delete,
    dat_init,
    dat_push,
    dat_pull,
    dat_rehash,
    dat_status,
    read_config,
    read_inventory,
    write_config,
    write_inventory,
)

BUCKET = "test-dat-bucket"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _md5(content: bytes) -> str:
    return hashlib.md5(content).hexdigest()


def make_file(path: str, content: bytes = b"hello") -> str:
    """Write content to path (creating parents as needed) and return its md5."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return _md5(content)


def put_master(s3, inventory: dict, bucket: str = BUCKET, hash_algo: str = "md5"):
    """Serialize inventory and upload it as .dat/master to the given bucket."""
    body = f"# hash: {hash_algo}\n" + "".join(f"{k}\t{v}\n" for k, v in sorted(inventory.items()))
    s3.put_object(Bucket=bucket, Key=".dat/master", Body=body.encode())


def put_s3_file(s3, key: str, content: bytes = b"data", bucket: str = BUCKET):
    s3.put_object(Bucket=bucket, Key=key, Body=content)


def bucket_keys(s3, bucket: str = BUCKET) -> set:
    resp = s3.list_objects_v2(Bucket=bucket)
    return {obj["Key"] for obj in resp.get("Contents", [])}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    """Ensure dummy AWS credentials are active for every test."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture()
def s3():
    """Start moto mock and create the shared test bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture()
def repo_dir(tmp_path, monkeypatch, s3):
    """Chdir into a temp dat repo whose config points at the mocked bucket."""
    monkeypatch.chdir(tmp_path)
    dat_dir = tmp_path / ".dat"
    dat_dir.mkdir()
    write_config({"aws": BUCKET, "hash": "md5", "pushed": "True", "symlinks": "ignore"}, dat_dir / "config")
    return tmp_path


# ---------------------------------------------------------------------------
# DatRepo.get_master
# ---------------------------------------------------------------------------

class TestGetMaster:
    def test_happy_path(self, repo_dir, s3):
        master = {"a.txt": "abc123", "b.txt": "def456"}
        put_master(s3, master)

        result = DatRepo().get_master()

        assert result == master

    def test_bucket_missing_never_pushed_returns_local(self, tmp_path, monkeypatch, s3):
        """NoSuchBucket + pushed==False: creates the bucket and returns local."""
        fresh = "brand-new-bucket"
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": fresh, "hash": "md5", "pushed": "False", "symlinks": "ignore"}, tmp_path / ".dat" / "config")
        local = {"a.txt": "abc123"}

        # Patch get_aws_region → None so create_bucket uses no LocationConstraint.
        monkeypatch.setattr(dat_module, "get_aws_region", lambda profile=None: None)

        repo = DatRepo()
        result = repo.get_master(local)

        assert result == local
        all_buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        assert fresh in all_buckets

    def test_404_never_pushed_no_local_dies(self, tmp_path, monkeypatch, s3):
        """404 + pushed==False + local=None → die with helpful message."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "hash": "md5", "pushed": "False", "symlinks": "ignore"}, tmp_path / ".dat" / "config")

        repo = DatRepo()
        with pytest.raises(SystemExit):
            repo.get_master()

    def test_credential_error_dies(self, tmp_path, monkeypatch):
        """NoCredentialsError during client construction causes sys.exit."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "hash": "md5", "pushed": "True", "symlinks": "ignore"}, tmp_path / ".dat" / "config")

        from botocore.exceptions import NoCredentialsError

        def _raise(*args, **kwargs):
            raise NoCredentialsError()

        monkeypatch.setattr(boto3, "client", _raise)
        repo = DatRepo()
        with pytest.raises(SystemExit):
            _ = repo.s3


# ---------------------------------------------------------------------------
# dat_push
# ---------------------------------------------------------------------------

class TestDatPush:
    def test_up_to_date_exits_cleanly(self, repo_dir, s3):
        content = b"already synced"
        h = make_file("a.txt", content)
        inventory = {"a.txt": h}
        write_inventory(inventory, repo_dir / ".dat" / "local", "md5")
        put_master(s3, inventory)

        with pytest.raises(SystemExit) as exc:
            dat_push()
        assert exc.value.code == 0

    def test_uploads_new_files(self, repo_dir, s3):
        make_file("data.txt", b"new file content")
        write_inventory({}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {})

        dat_push()

        keys = bucket_keys(s3)
        assert "data.txt" in keys
        assert ".dat/master" in keys
        assert "data.txt" in read_inventory()

    def test_deletes_purged_files(self, repo_dir, s3):
        """A file deleted locally (but tracked) should be removed from S3."""
        h_a = make_file("a.txt", b"file a")
        # b.txt is tracked in local/master but does not exist on disk
        old_inv = {"a.txt": h_a, "b.txt": "oldhash"}
        write_inventory(old_inv, repo_dir / ".dat" / "local", "md5")
        put_master(s3, old_inv)
        put_s3_file(s3, "b.txt", b"old content")

        dat_push()

        keys = bucket_keys(s3)
        # a.txt was unchanged so it stays on S3 only if it was there before; the
        # important assertion is that the purged file was removed.
        assert "b.txt" not in keys
        assert ".dat/master" in keys

    def test_dry_run_does_not_upload(self, repo_dir, s3):
        make_file("dry.txt", b"data")
        write_inventory({}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {})

        dat_push(dry=True)

        assert "dry.txt" not in bucket_keys(s3)

    def test_first_push_sets_pushed_true(self, tmp_path, monkeypatch, s3):
        """First ever push (pushed==False) sets pushed=True in config after success."""
        fresh = "brand-new-bucket-push"
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": fresh, "hash": "md5", "pushed": "False", "symlinks": "ignore"}, tmp_path / ".dat" / "config")
        make_file("hello.txt", b"hello")
        write_inventory({}, tmp_path / ".dat" / "local", "md5")
        monkeypatch.setattr(dat_module, "get_aws_region", lambda profile=None: None)

        dat_push()

        config = read_config(tmp_path / ".dat" / "config")
        assert config["pushed"] == "True"
        assert "hello.txt" in bucket_keys(s3, fresh)


# ---------------------------------------------------------------------------
# dat_pull
# ---------------------------------------------------------------------------

class TestDatPull:
    def test_up_to_date_exits_cleanly(self, repo_dir, s3):
        content = b"already synced"
        h = make_file("a.txt", content)
        inventory = {"a.txt": h}
        write_inventory(inventory, repo_dir / ".dat" / "local", "md5")
        put_master(s3, inventory)

        with pytest.raises(SystemExit) as exc:
            dat_pull()
        assert exc.value.code == 0

    def test_downloads_new_remote_files(self, repo_dir, s3):
        content = b"from the cloud"
        h = _md5(content)
        put_s3_file(s3, "remote.txt", content)
        put_master(s3, {"remote.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local", "md5")

        dat_pull()

        assert Path("remote.txt").read_bytes() == content
        assert read_inventory().get("remote.txt") == h

    def test_removes_killed_local_files(self, repo_dir, s3):
        """A file deleted from master should be removed locally."""
        h = make_file("stale.txt", b"stale local copy")
        write_inventory({"stale.txt": h}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {})  # master no longer tracks stale.txt → kill

        dat_pull()

        assert not Path("stale.txt").exists()
        assert "stale.txt" not in read_inventory()

    def test_dry_run_does_not_download(self, repo_dir, s3):
        content = b"would be downloaded"
        h = _md5(content)
        put_s3_file(s3, "dry.txt", content)
        put_master(s3, {"dry.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local", "md5")

        dat_pull(dry=True)

        assert not Path("dry.txt").exists()


# ---------------------------------------------------------------------------
# dat_checkin / dat_checkout
# ---------------------------------------------------------------------------

class TestCheckinCheckout:
    def test_checkin_uploads_file_and_updates_inventories(self, repo_dir, s3):
        content = b"important dataset"
        h = make_file("data.csv", content)
        write_inventory({}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {})

        dat_checkin("data.csv")

        keys = bucket_keys(s3)
        assert "data.csv" in keys
        assert ".dat/master" in keys
        assert read_inventory().get("data.csv") == h

    def test_checkin_missing_file_dies(self, repo_dir, s3):
        with pytest.raises(SystemExit):
            dat_checkin("nonexistent.txt")

    def test_checkout_downloads_file_and_updates_local(self, repo_dir, s3):
        content = b"remote dataset"
        h = _md5(content)
        put_s3_file(s3, "data.csv", content)
        write_inventory({}, repo_dir / ".dat" / "local", "md5")

        dat_checkout("data.csv")

        assert Path("data.csv").read_bytes() == content
        assert read_inventory().get("data.csv") == h


# ---------------------------------------------------------------------------
# dat_status
# ---------------------------------------------------------------------------

class TestDatStatus:
    def test_local_nothing_to_push(self, repo_dir, s3, capsys):
        content = b"synced"
        h = make_file("a.txt", content)
        write_inventory({"a.txt": h}, repo_dir / ".dat" / "local", "md5")

        dat_status(remote=False)

        assert "clean" in capsys.readouterr().out

    def test_local_modified(self, repo_dir, s3, capsys):
        make_file("a.txt", b"new content")
        write_inventory({"a.txt": "oldhash"}, repo_dir / ".dat" / "local", "md5")

        dat_status(remote=False)

        assert "a.txt" in capsys.readouterr().out

    def test_local_deleted(self, repo_dir, s3, capsys):
        # b.txt in local inventory but not on disk
        write_inventory({"b.txt": "somehash"}, repo_dir / ".dat" / "local", "md5")

        dat_status(remote=False)

        assert "b.txt" in capsys.readouterr().out

    def test_remote_new_file(self, repo_dir, s3, capsys):
        content = b"updated remotely"
        h = _md5(content)
        put_s3_file(s3, "remote.txt", content)
        put_master(s3, {"remote.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local", "md5")

        dat_status(remote=True)

        assert "remote.txt" in capsys.readouterr().out

    def test_remote_in_sync(self, repo_dir, s3, capsys):
        content = b"synced"
        h = make_file("a.txt", content)
        inventory = {"a.txt": h}
        write_inventory(inventory, repo_dir / ".dat" / "local", "md5")
        put_master(s3, inventory)

        dat_status(remote=True)

        assert "current" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# dat_init
# ---------------------------------------------------------------------------

class TestDatInit:
    def test_creates_dat_dir_and_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        dat_init("my-bucket", profile=None)

        config = read_config(tmp_path / ".dat" / "config")
        assert config["aws"] == "my-bucket"
        assert config["pushed"] == "False"
        assert config["hash"] == "xxh3_64"  # xxhash is installed → default is xxh3_64
        assert config["symlinks"] == "ignore"

    def test_creates_ignore_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        dat_init("my-bucket", profile=None)

        ignore = (tmp_path / ".dat" / "ignore").read_text()
        assert ".DS_Store" in ignore

    def test_stores_profile_in_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        dat_init("my-bucket", profile="my-profile")

        config = read_config(tmp_path / ".dat" / "config")
        assert config["profile"] == "my-profile"

    def test_autogenerates_bucket_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("getpass.getuser", lambda: "testuser")

        dat_init(None, profile=None)

        config = read_config(tmp_path / ".dat" / "config")
        assert config["aws"].startswith("testuser.")

    def test_fails_if_dat_dir_exists(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()

        with pytest.raises(SystemExit):
            dat_init("my-bucket", profile=None)


# ---------------------------------------------------------------------------
# dat_clone
# ---------------------------------------------------------------------------

class TestDatClone:
    def test_downloads_files_and_writes_config(self, tmp_path, monkeypatch, s3):
        monkeypatch.chdir(tmp_path)
        content = b"dataset content"
        h = _md5(content)
        put_s3_file(s3, "data.csv", content)
        put_master(s3, {"data.csv": h})

        dat_clone(BUCKET, "cloned")

        assert (tmp_path / "cloned" / "data.csv").read_bytes() == content
        config = read_config(tmp_path / "cloned" / ".dat" / "config")
        assert config["aws"] == BUCKET
        assert config["pushed"] == "True"

    def test_master_renamed_to_local(self, tmp_path, monkeypatch, s3):
        monkeypatch.chdir(tmp_path)
        put_master(s3, {"a.txt": "abc"})

        dat_clone(BUCKET, "cloned")

        assert (tmp_path / "cloned" / ".dat" / "local").is_file()
        assert not (tmp_path / "cloned" / ".dat" / "master").exists()

    def test_folder_defaults_to_bucket_name(self, tmp_path, monkeypatch, s3):
        monkeypatch.chdir(tmp_path)
        put_master(s3, {})

        dat_clone(BUCKET, None)

        assert (tmp_path / BUCKET).is_dir()

    def test_fails_if_folder_exists(self, tmp_path, monkeypatch, s3):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "existing").mkdir()

        with pytest.raises(SystemExit):
            dat_clone(BUCKET, "existing")

    def test_inherits_hash_from_remote_config(self, tmp_path, monkeypatch, s3):
        """Clone picks up the remote repo's hash algorithm."""
        monkeypatch.chdir(tmp_path)
        remote_cfg = "aws: test-dat-bucket\nhash: xxh3_64\npushed: True\nsymlinks: ignore\n"
        s3.put_object(Bucket=BUCKET, Key=".dat/config", Body=remote_cfg.encode())
        put_master(s3, {})

        dat_clone(BUCKET, "cloned")

        config = read_config(tmp_path / "cloned" / ".dat" / "config")
        assert config["hash"] == "xxh3_64"

    def test_falls_back_to_md5_when_no_remote_config(self, tmp_path, monkeypatch, s3):
        """Clone defaults to md5 when the remote has no .dat/config (old repo)."""
        monkeypatch.chdir(tmp_path)
        put_master(s3, {})

        dat_clone(BUCKET, "cloned")

        config = read_config(tmp_path / "cloned" / ".dat" / "config")
        assert config["hash"] == "md5"


# ---------------------------------------------------------------------------
# dat_delete
# ---------------------------------------------------------------------------

class TestDatDelete:
    def test_deletes_bucket_and_dat_dir(self, repo_dir, s3):
        put_s3_file(s3, "data.txt", b"some data")
        put_master(s3, {"data.txt": _md5(b"some data")})

        dat_delete()

        all_buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        assert BUCKET not in all_buckets
        assert not (repo_dir / ".dat").exists()

    def test_prefix_config_only_removes_dat_dir(self, tmp_path, monkeypatch, s3):
        """When aws contains a prefix (/), skip S3 deletion and only remove .dat/."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": f"{BUCKET}/subdir", "hash": "md5", "pushed": "True", "symlinks": "ignore"}, tmp_path / ".dat" / "config")
        put_s3_file(s3, "subdir/data.txt", b"data")

        dat_delete()

        assert not (tmp_path / ".dat").exists()
        assert "subdir/data.txt" in bucket_keys(s3)

    def test_fails_if_bucket_missing(self, repo_dir, s3):
        s3.delete_bucket(Bucket=BUCKET)

        with pytest.raises(SystemExit):
            dat_delete()


# ---------------------------------------------------------------------------
# config_repair
# ---------------------------------------------------------------------------

class TestConfigRepair:
    def test_adds_hash_and_symlinks_to_old_config(self, tmp_path, monkeypatch, s3):
        """A repo with no hash/symlinks keys gets them added on first DatRepo use."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "pushed": "True"}, tmp_path / ".dat" / "config")

        DatRepo()

        config = read_config(tmp_path / ".dat" / "config")
        assert config["hash"] == "md5"
        assert config["symlinks"] == "ignore"

    def test_symlinks_follow_when_symlinks_exist(self, tmp_path, monkeypatch, s3):
        """If the repo has symlinks, repair sets symlinks: follow for compatibility."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "pushed": "True"}, tmp_path / ".dat" / "config")
        target = tmp_path / "real.txt"
        target.write_bytes(b"data")
        (tmp_path / "link.txt").symlink_to(target)

        DatRepo()

        config = read_config(tmp_path / ".dat" / "config")
        assert config["symlinks"] == "follow"

    def test_repair_is_idempotent(self, tmp_path, monkeypatch, s3):
        """Running DatRepo twice does not change an already-repaired config."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "pushed": "True"}, tmp_path / ".dat" / "config")

        DatRepo()
        mtime_after_first = (tmp_path / ".dat" / "config").stat().st_mtime
        DatRepo()
        mtime_after_second = (tmp_path / ".dat" / "config").stat().st_mtime

        assert mtime_after_first == mtime_after_second


# ---------------------------------------------------------------------------
# symlink handling in take_inventory
# ---------------------------------------------------------------------------

class TestSymlinks:
    def test_ignore_skips_symlinked_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "real.txt"
        target.write_bytes(b"data")
        (tmp_path / "link.txt").symlink_to(target)

        inv = dat_module.take_inventory({"symlinks": "ignore"}, root=tmp_path)

        assert "real.txt" in inv
        assert "link.txt" not in inv

    def test_ignore_skips_symlinked_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        real_dir = tmp_path / "real_dir"
        real_dir.mkdir()
        (real_dir / "file.txt").write_bytes(b"data")
        (tmp_path / "linked_dir").symlink_to(real_dir)

        inv = dat_module.take_inventory({"symlinks": "ignore"}, root=tmp_path)

        assert "real_dir/file.txt" in inv
        assert "linked_dir/file.txt" not in inv

    def test_follow_includes_symlinked_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "real.txt"
        target.write_bytes(b"data")
        (tmp_path / "link.txt").symlink_to(target)

        inv = dat_module.take_inventory({"symlinks": "follow"}, root=tmp_path)

        assert "real.txt" in inv
        assert "link.txt" in inv

    def test_follow_includes_symlinked_directory_contents(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        real_dir = tmp_path / "real_dir"
        real_dir.mkdir()
        (real_dir / "file.txt").write_bytes(b"data")
        (tmp_path / "linked_dir").symlink_to(real_dir)

        inv = dat_module.take_inventory({"symlinks": "follow"}, root=tmp_path)

        assert "linked_dir/file.txt" in inv


# ---------------------------------------------------------------------------
# dat_rehash
# ---------------------------------------------------------------------------

def _xxh3_64(content: bytes) -> str:
    import xxhash
    return xxhash.xxh3_64(content).hexdigest()


class TestDatRehash:
    def _clean_repo(self, repo_dir, s3, content=b"test data"):
        """Set up a clean md5 repo with one file pushed."""
        h = make_file("data.txt", content)
        write_inventory({"data.txt": h}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {"data.txt": h})
        return h

    def test_converts_md5_to_xxh3_64(self, repo_dir, s3, monkeypatch):
        content = b"test data"
        self._clean_repo(repo_dir, s3, content)
        monkeypatch.setattr("builtins.input", lambda _: "")  # press enter = y

        dat_rehash("xxh3_64")

        config = read_config(repo_dir / ".dat" / "config")
        assert config["hash"] == "xxh3_64"
        local = read_inventory()
        assert local["data.txt"] == _xxh3_64(content)
        # Verify master on S3 was updated
        resp = s3.get_object(Bucket=BUCKET, Key=".dat/master")
        lines = resp["Body"].read().decode().strip().split("\n")
        data_lines = [l for l in lines if not l.startswith("#")]
        for line in data_lines:
            _, digest = line.split("\t")
            assert len(digest) == 16  # xxh3_64 hex is 16 chars

    def test_xxhash_alias_normalizes_to_xxh3_64(self, repo_dir, s3, monkeypatch):
        self._clean_repo(repo_dir, s3)
        monkeypatch.setattr("builtins.input", lambda _: "")

        dat_rehash("xxhash")

        config = read_config(repo_dir / ".dat" / "config")
        assert config["hash"] == "xxh3_64"

    def test_no_op_when_already_on_target_algo(self, repo_dir, s3, capsys):
        dat_rehash("md5")  # repo_dir is already md5

        assert "nothing to do" in capsys.readouterr().out
        config = read_config(repo_dir / ".dat" / "config")
        assert config["hash"] == "md5"

    def test_aborts_if_push_needed(self, repo_dir, s3):
        make_file("new.txt", b"unpushed file")
        write_inventory({}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {})

        with pytest.raises(SystemExit):
            dat_rehash("xxh3_64")

    def test_aborts_if_purge_needed(self, repo_dir, s3):
        # b.txt tracked in local but deleted from disk
        h = make_file("a.txt", b"a")
        write_inventory({"a.txt": h, "b.txt": "oldhash"}, repo_dir / ".dat" / "local", "md5")
        put_master(s3, {"a.txt": h, "b.txt": "oldhash"})

        with pytest.raises(SystemExit):
            dat_rehash("xxh3_64")

    def test_dry_run_makes_no_changes(self, repo_dir, s3, capsys):
        content = b"data"
        self._clean_repo(repo_dir, s3, content)

        dat_rehash("xxh3_64", dry=True)

        config = read_config(repo_dir / ".dat" / "config")
        assert config["hash"] == "md5"
        local = read_inventory()
        assert local["data.txt"] == _md5(content)
        assert "Would rehash" in capsys.readouterr().out

    def test_dry_run_notes_unsynced_remote_changes(self, repo_dir, s3, capsys):
        content = b"data"
        h = make_file("a.txt", content)
        write_inventory({"a.txt": h}, repo_dir / ".dat" / "local", "md5")
        # Master has a new file that local doesn't know about
        put_master(s3, {"a.txt": h, "b.txt": "remotehash"})

        dat_rehash("xxh3_64", dry=True)

        out = capsys.readouterr().out
        assert "remote has unsynced changes" in out
        assert "Would rehash" in out

    def test_aborts_if_user_declines_confirmation(self, repo_dir, s3, monkeypatch):
        self._clean_repo(repo_dir, s3)
        monkeypatch.setattr("builtins.input", lambda _: "n")

        with pytest.raises(SystemExit):
            dat_rehash("xxh3_64")
