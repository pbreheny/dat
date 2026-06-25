[![GitHub version](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pbreheny/dat/master/.version.json&style=flat&logo=github)](https://github.com/pbreheny/dat)

# dat: Push/pull sync with minimal overhead

`git` is great for code, but is less useful with other types of files: large files cause the repo to balloon in size, many of its tools like diff and merge only work well with text, and private data is problematic to store on GitHub. `dat` gives you a `git`-style push/pull workflow backed by AWS S3, without the version history overhead. It is particularly useful for large binary files like images, PDFs, and serialized data files.

The key difference from simply using rsync or copying files to S3 directly: `dat` detects conflicts. If a collaborator has modified a file you're about to overwrite, `dat` flags it rather than silently clobbering their work. You get safe multi-user coordination without a version control system.

Under the hood, `dat` computes the (xxh3_64) hash of every file and only transfers data when content has changed. This means that syncing operations are fast even when directories are large.

## Installation

```bash
pip install git+https://github.com/pbreheny/dat
```

`dat` requires an AWS account with S3 access. Credentials can be configured via environment variables, `~/.aws/credentials`, or any other method supported by `boto3`.

## Basic usage

Put the files you want to track in a directory. From that directory, run

```bash
dat init
dat push
```

By default, this will create a bucket called `username.path.to.dir`. For example, if your username is `jsmith7` and your `dat` directory is `HOME/pdf/articles`, the default bucket name is `jsmith7.pdf.articles`. Alternatively, you can run `dat init <bucket>` to specify your own bucket name. You can also use something like `jsmith7.pdf/articles` if the bucket `jsmith7.pdf` already exists and you want the `articles` folder to mirror the `articles` subdirectory of that bucket.

To clone the repository on a new machine:

```bash
dat clone jsmith7.pdf.articles articles
```

The first argument is the bucket name; the second is the local folder name (defaults to the bucket name if omitted).

To pull changes from the remote:

```bash
dat pull
```

To check if any local files need pushing (offline, no AWS connection needed):

```bash
dat status
```

To check against the remote to see if anything needs pushing or pulling:

```bash
dat status -r
```

## Sharing a bucket with another AWS account

To grant another AWS user access to your bucket:

```bash
dat share <account_number> <username>
```

This updates the bucket policy to grant the specified IAM user `GetObject`, `PutObject`, `DeleteObject`, and `ListBucket` permissions. To share with the root of an account instead of a specific IAM user:

```bash
dat share <account_number> --root
```
