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


def put_master(s3, inventory: dict, bucket: str = BUCKET):
    """Serialize inventory and upload it as .dat/master to the given bucket."""
    body = "".join(f"{k}\t{v}\n" for k, v in sorted(inventory.items()))
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
    write_config({"aws": BUCKET, "pushed": "True"}, dat_dir / "config")
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
        write_config({"aws": fresh, "pushed": "False"}, tmp_path / ".dat" / "config")
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
        write_config({"aws": BUCKET, "pushed": "False"}, tmp_path / ".dat" / "config")

        repo = DatRepo()
        with pytest.raises(SystemExit):
            repo.get_master()

    def test_credential_error_dies(self, tmp_path, monkeypatch):
        """NoCredentialsError during client construction causes sys.exit."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": BUCKET, "pushed": "True"}, tmp_path / ".dat" / "config")

        from botocore.exceptions import NoCredentialsError

        def _raise(*args, **kwargs):
            raise NoCredentialsError()

        monkeypatch.setattr(boto3, "client", _raise)
        with pytest.raises(SystemExit):
            DatRepo()


# ---------------------------------------------------------------------------
# dat_push
# ---------------------------------------------------------------------------

class TestDatPush:
    def test_up_to_date_exits_cleanly(self, repo_dir, s3):
        content = b"already synced"
        h = make_file("a.txt", content)
        inventory = {"a.txt": h}
        write_inventory(inventory, repo_dir / ".dat" / "local")
        put_master(s3, inventory)

        with pytest.raises(SystemExit) as exc:
            dat_push()
        assert exc.value.code == 0

    def test_uploads_new_files(self, repo_dir, s3):
        make_file("data.txt", b"new file content")
        write_inventory({}, repo_dir / ".dat" / "local")
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
        write_inventory(old_inv, repo_dir / ".dat" / "local")
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
        write_inventory({}, repo_dir / ".dat" / "local")
        put_master(s3, {})

        dat_push(dry=True)

        assert "dry.txt" not in bucket_keys(s3)

    def test_first_push_sets_pushed_true(self, tmp_path, monkeypatch, s3):
        """First ever push (pushed==False) sets pushed=True in config after success."""
        fresh = "brand-new-bucket-push"
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dat").mkdir()
        write_config({"aws": fresh, "pushed": "False"}, tmp_path / ".dat" / "config")
        make_file("hello.txt", b"hello")
        write_inventory({}, tmp_path / ".dat" / "local")
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
        write_inventory(inventory, repo_dir / ".dat" / "local")
        put_master(s3, inventory)

        with pytest.raises(SystemExit) as exc:
            dat_pull()
        assert exc.value.code == 0

    def test_downloads_new_remote_files(self, repo_dir, s3):
        content = b"from the cloud"
        h = _md5(content)
        put_s3_file(s3, "remote.txt", content)
        put_master(s3, {"remote.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local")

        dat_pull()

        assert Path("remote.txt").read_bytes() == content
        assert read_inventory().get("remote.txt") == h

    def test_removes_killed_local_files(self, repo_dir, s3):
        """A file deleted from master should be removed locally."""
        h = make_file("stale.txt", b"stale local copy")
        write_inventory({"stale.txt": h}, repo_dir / ".dat" / "local")
        put_master(s3, {})  # master no longer tracks stale.txt → kill

        dat_pull()

        assert not Path("stale.txt").exists()
        assert "stale.txt" not in read_inventory()

    def test_dry_run_does_not_download(self, repo_dir, s3):
        content = b"would be downloaded"
        h = _md5(content)
        put_s3_file(s3, "dry.txt", content)
        put_master(s3, {"dry.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local")

        dat_pull(dry=True)

        assert not Path("dry.txt").exists()


# ---------------------------------------------------------------------------
# dat_checkin / dat_checkout
# ---------------------------------------------------------------------------

class TestCheckinCheckout:
    def test_checkin_uploads_file_and_updates_inventories(self, repo_dir, s3):
        content = b"important dataset"
        h = make_file("data.csv", content)
        write_inventory({}, repo_dir / ".dat" / "local")
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
        write_inventory({}, repo_dir / ".dat" / "local")

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
        write_inventory({"a.txt": h}, repo_dir / ".dat" / "local")

        dat_status(remote=False)

        assert "clean" in capsys.readouterr().out

    def test_local_modified(self, repo_dir, s3, capsys):
        make_file("a.txt", b"new content")
        write_inventory({"a.txt": "oldhash"}, repo_dir / ".dat" / "local")

        dat_status(remote=False)

        assert "a.txt" in capsys.readouterr().out

    def test_local_deleted(self, repo_dir, s3, capsys):
        # b.txt in local inventory but not on disk
        write_inventory({"b.txt": "somehash"}, repo_dir / ".dat" / "local")

        dat_status(remote=False)

        assert "b.txt" in capsys.readouterr().out

    def test_remote_new_file(self, repo_dir, s3, capsys):
        content = b"updated remotely"
        h = _md5(content)
        put_s3_file(s3, "remote.txt", content)
        put_master(s3, {"remote.txt": h})
        write_inventory({}, repo_dir / ".dat" / "local")

        dat_status(remote=True)

        assert "remote.txt" in capsys.readouterr().out

    def test_remote_in_sync(self, repo_dir, s3, capsys):
        content = b"synced"
        h = make_file("a.txt", content)
        inventory = {"a.txt": h}
        write_inventory(inventory, repo_dir / ".dat" / "local")
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
        write_config({"aws": f"{BUCKET}/subdir", "pushed": "True"}, tmp_path / ".dat" / "config")
        put_s3_file(s3, "subdir/data.txt", b"data")

        dat_delete()

        assert not (tmp_path / ".dat").exists()
        assert "subdir/data.txt" in bucket_keys(s3)

    def test_fails_if_bucket_missing(self, repo_dir, s3):
        s3.delete_bucket(Bucket=BUCKET)

        with pytest.raises(SystemExit):
            dat_delete()
