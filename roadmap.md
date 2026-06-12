# dat improvement roadmap

## Background

`dat` is a push/pull sync tool for large binary files backed by AWS S3 — like git, but without version control. The goal of this roadmap is to make the package more robust, testable, and portable.

## Phase 1: Core refactoring

### Step 1: Replace `os.system()` / AWS CLI with boto3 (done)

The package was shelling out to the `aws` CLI for all S3 operations, creating a hidden system dependency and making testing nearly impossible. All S3 calls now go through `boto3` directly. The `aws` CLI is no longer required.

Concretely replaced:
- `get_master`: `aws s3 cp` → `s3.download_file`
- `dat_checkin`: `aws s3 sync` → `s3.upload_file` (file + master)
- `dat_checkout`: `aws s3 cp` → `s3.download_file`
- `dat_clone`: `aws sts get-caller-identity` + `aws s3 sync` → boto3 STS + paginator download
- `dat_delete`: `aws s3 rm --recursive` → paginator + `s3.delete_objects`
- `dat_overwrite_master`: `aws s3 sync --delete` → upload all + paginator delete of extras
- `dat_pull`: `aws s3 sync` → per-file `s3.download_file` / `os.remove`
- `dat_push`: `aws s3 sync` + `aws s3 rm` → per-file `s3.upload_file` / `s3.delete_object`
- `dat_repair_master`: `aws s3 cp --recursive` + `aws s3 sync` → `_download_all` helper + `s3.upload_file`
- `get_aws_region`: `aws configure get region` → `boto3.Session().region_name`

New helpers: `_s3_client(config)`, `_parse_bucket(aws_str)`, `_full_key(prefix, path)`, `_download_all(s3, bucket, prefix, dest_dir)`.

### Step 2: Introduce a `DatRepo` class

Eliminate the pattern where every function re-reads the config and creates a new S3 client. A `DatRepo` class should own `config`, `s3`, `bucket`, and `prefix` as attributes.

### Step 3: Standardize paths with `pathlib`

Replace string concatenation of `.dat/config`, `.dat/local`, etc. with `pathlib.Path` constants or `DatRepo` properties.

### Step 4: Unify error handling

Replace the mix of `quit()`, `sys.exit()`, and bare exceptions with a single `die(msg)` helper.

---

## Phase 2: Remove docopt → argparse

Swap `docopt` for stdlib `argparse` (subparsers). Removes one runtime dependency and makes the package more portable. Logic is unchanged.

---

## Phase 3: Testing

Fully automate all existing tests. Then proceed in three steps.

### Step 1: Inventory functions (no mocking needed)

- `read_inventory` / `write_inventory`: round-trip test, empty file, missing file
- `take_inventory`: ignore patterns, nested subdirectories, symlinks ignored
- `read_config` / `write_config`: round-trip, missing file calls die()

### Step 2: S3 integration via moto (mock AWS)

The core idea: moto intercepts boto3 calls, so you can create a fake S3 bucket in a test, populate it with objects, run dat_push/dat_pull, and assert on the resulting bucket/local state — all without a real AWS account.

- `dat_push`: set up a fake bucket + local files + inventories, run push, assert correct objects uploaded/deleted
- `dat_pull`: populate fake bucket, run pull, assert correct files created/removed locally
- `dat_checkin` / `dat_checkout`: simpler single-file versions of push/pull
- `dat_status`: assert output lines match expected state
- `DatRepo.get_master`: 404 path (never pushed), happy path, credential error path

### Step 3: End-to-end command dispatch

- `dat_init`: verify .dat/ directory and config created correctly
- `dat_clone`: fake bucket → local directory created with right files
- `dat_delete`: verify bucket objects and .dat/ removed

The moto tests will use a shared pytest.fixture that spins up a fake S3 bucket, writes `.dat/config` pointing at it, and tears down after each test. A separate fixture would create a known local file tree + inventory state to test push/pull against.

---

## Phase 4: Faster hashing

Replace MD5 with `xxhash` (specifically `xxh3_64`) for large-file performance. Store the hash algorithm in `.dat/config` for new repos. Existing repos without the key continue to use MD5. Add `xxhash` as an optional dependency with graceful fallback.

---

## Phase 5: Portability / "official" cleanup

- Remove or clearly mark HPC-specific code in `dat_clone()` (the `hpc:` path with hardcoded `/Shared/Fisher/hub/` paths)
- Audit `dat_share()` — specific to multi-account AWS setups; decide if it belongs in core or docs
- Add type hints to public functions
- Clean up README to remove personal-setup assumptions
- Audit for any hardcoded region or account assumptions
