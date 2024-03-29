[![GitHub version](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pbreheny/dat/master/.version.json&style=flat&logo=github)](https://github.com/pbreheny/dat)

# dat: A push/pull system for keeping files synchronized without version control

Motivation: `git` is great, but if you have lots of big binary files, the `git` repo becomes enormous. Furthermore, if these files include private data, you might not want to host them on GitHub. `dat` allows you to push and pull like `git`, but with very little overhead (because it doesn't do version control).

The basic logic behind `dat` is that it tracks the [md5](https://en.wikipedia.org/wiki/MD5) hash of all the files in a `dat` repo and only pushes/pulls if that md5 hash has changed.

## Installation

```bash
pip install git+https://github.com/pbreheny/dat
```

`dat` also requires that the [AWS command-line interface](https://aws.amazon.com/cli/) is installed on your system.

## Basic usage

Put the files you want to track in a directory. From that directory, run

```bash
dat init
dat push
```

By default, this will create a bucket called `username.path.to.dir`. For example, if your username is jsmith7 and your `dat` directory is `HOME/pdf/articles`, the default bucket name is `jsmith7.pdf.artcles`. Alternatively, you can run `dat init <bucket>` and specify your own bucket name. In particular, you can specify something like `jsmith7.pdf/articles` if the bucket `jsmith7.pdf` already exists and you want the `articles` folder on your local machine to mirror the `articles` subdirectory of that bucket.

If you are on a different machine (still with username `jsmith7`) and go to the `pdf` folder, you can then run

```bash
dat clone articles
```

to clone the repository on the new machine. Alternatively, `dat clone <bucket> <folder>` allows you to clone an arbitrary bucket into an arbitrary folder.

To pull changes from the master bucket,

```bash
dat pull
```

To check if any local files need pushing (can be done offline, doesn't require AWS connection)

```bash
dat status
```

To check against the remote to see if anything needs pushing or pulling

```bash
dat -r status
```

## AWS permissions

When using `dat` to share a repository between AWS accounts, be aware that in addition to S3 `GetObject` permissions on the objects in the bucket, you also need to grant `ListBucket` permissions for `dat` to work because `aws s3 sync` needs to list all the objects in the bucket to determine which files need to be copied over.

Specifically, your S3 bucket policy statement needs to look something like this:

``` json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789101:user/Username"
            },
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::my.s3.bucket/*"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123456789101:user/Username"
            },
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::my.s3.bucket"
        }
    ]
}
```

## To do

* Need better tools for resolving conflicts, like `ours` / `theirs` in `git`
* Should institute some sort of lock so that two operations can't do `dat` things at the same time.
* Limited testing by users other than me
* Currently specific to AWS, would be neat if it worked with, say, Google Drive as well.
