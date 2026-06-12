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

With boto3 calls instead of shell calls, AWS can be mocked via `moto` or `unittest.mock`.

Target coverage:
- Inventory functions (`read_inventory`, `write_inventory`, `take_inventory`)
- Change detection (`needs_push`, `needs_pull`, `needs_purge`, `needs_kill`)
- Conflict resolution (all four `resolve_*` functions) — fixture-based, already partially started
- Push/pull logic with mocked S3 (integration-style via `moto`)

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
