[![GitHub version](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pbreheny/dat/master/.version.json&style=flat&logo=github)](https://github.com/pbreheny/dat)

# dat: A push/pull system for keeping files synchronized without version control

Motivation: `git` is great, but if you have lots of big binary files, the `git` repo becomes enormous. Furthermore, if these files include private data, you might not want to host them on GitHub. `dat` allows you to push and pull like `git`, but with very little overhead (because it doesn't do version control).

The basic logic behind `dat` is that it tracks the [md5](https://en.wikipedia.org/wiki/MD5) hash of all the files in a `dat` repo and only pushes/pulls if that md5 hash has changed.

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

## To do

* Need better tools for resolving conflicts, like `ours` / `theirs` in `git`
* Should institute some sort of lock to prevent concurrent `dat` operations
