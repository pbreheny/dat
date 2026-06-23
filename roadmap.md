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

## Phase 4: Portability / "official" cleanup

- Remove or clearly mark HPC-specific code in `dat_clone()` (the `hpc:` path with hardcoded `/Shared/Fisher/hub/` paths)
- Audit `dat_share()` — specific to multi-account AWS setups; decide if it belongs in core or docs
- Add type hints to public functions
- Clean up README to remove personal-setup assumptions
- Audit for any hardcoded region or account assumptions

---

## Phase 5: Faster hashing

Replace MD5 with `xxhash` (specifically `xxh3_64`) for large-file performance. Store the hash algorithm in `.dat/config` for new repos. Existing repos without the key continue to use MD5. Add `xxhash` as an optional dependency with graceful fallback.

### Phase 1 — Hash dispatch (foundational)

Replace the hardcoded md5() call with a dispatcher. The only caller is take_inventory() at line 308.

- Rename md5() → _hash_md5(fname) (private helper)
- Add _hash_xxh3(fname) that uses the streaming xxhash API (same chunk loop pattern as md5); wrap the import xxhash in a module-level try/except so that missing xxhash degrades gracefully with a clear error at call time
- Add hash_file(fname, algorithm) that maps "md5" → _hash_md5 and "xxhash" → _hash_xxh3
- Change take_inventory() to accept config (it already does) and call hash_file(path, config["hash"]) instead of md5(path) directly

Nothing else changes behavior yet — existing hash: md5 configs continue working identically.

### Phase 2 — New-repo defaults

Two places hardcode hash: md5:

dat_init (line 657): detect xxhash availability at runtime and write hash: xxhash if it's importable, hash: md5 otherwise.

dat_clone (lines 613–616): this currently discards the remote .dat/config and writes a new one with hash: md5. Instead, read the downloaded remote config first (it was placed in folder_path/.dat/config by _download_all), extract its hash key, and carry that into the new local config. Fall back to md5 if the key is absent (old remote repo).

config_repair keeps its hash: md5 default — correct, since old repos that lack the key were hashed with md5.

### Phase 3 — dat rehash conversion command

This is the trickiest piece. The goal: convert a repo's inventories from md5 to xxhash digests. The inventories (.dat/local and .dat/master) contain the hashes; the actual S3 file objects don't change.

Command: dat rehash

Flow:
1. Refuse to run if config["hash"] is already xxhash (nothing to do)
2. Validate clean state: compute md5 inventory of local files, compare with .dat/local; if anything is locally modified or needs push/pull, abort with a message — you want a clean baseline before converting
3. Compute xxhash inventory of all local files
4. Download remote master from S3
  - If master digests are already 16-char hex (xxhash length) rather than 32-char (md5): another collaborator already ran rehash. In this case, only update .dat/local and .dat/config — no push
  - Otherwise: build new master by substituting the xxhash values (files present locally get new hashes; files in master but absent locally — e.g. never pulled — should cause an abort/warning, since we can't rehash what we don't have)
5. Push new master to S3
6. Write new .dat/local with xxhash values
7. Update .dat/config to hash: xxhash and save

Collaborator flow: after the first collaborator runs dat rehash and pushes, a second collstep 4 detects the remote is already xxhash → local-only update, no redundant push.

What if someone runs dat pull before running dat rehash? Their local has md5, master has xxhash — every file looks "changed." We should add a hash-length sniff in dat_pull (or DatRepo.get_master) to detect
this mismatch and die with a clear message: "Remote repository uses xxhash but local confto upgrade."

### Phase 4 — pyproject.toml and tests

- Add [project.optional-dependencies] with fast = ["xxhash"] so users can do pip install
- Tests in test_core_logic.py:
  - hash_file dispatches correctly for both algorithms
  - take_inventory uses the algorithm from config
- Tests in test_s3.py:
  - dat_init writes hash: xxhash when xxhash is available (mock the import)
  - dat_clone inherits the remote's hash algorithm
  - dat rehash happy path: md5 → xxhash, local+master updated
  - dat rehash collaborator path: master already xxhash → local-only update
  - dat pull mismatch detection

---
Open questions before we start

1. xxhash flavor: I'd recommend xxh3_64 (16-char hex, fastest). xxh3_128 would produce 32der to distinguish by length — useful if you want the sniff in phase 3 to work cleanly but
not strictly necessary. Preference?
2. Repos with files in master not present locally: Should dat rehash abort (safe) or warng master in a mixed-algorithm state? I'd lean toward aborting with a message to run datpull first.
3. Config label: The config key would store "xxhash" as the value. That's an alias for xxfine, or do you want "xxh3_64" to be more explicit?


Safety must be enforced -- user cannot rehash unless dat repo is in a "clean" state. In fact, since the usual dat check is local-only, I think the user should be prompted to run `dat pull`
