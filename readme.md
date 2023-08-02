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

By default, this will create a bucket called `username.path.to.dir`. For example, if your username is jsmith7 and your `dat` directory is `HOME/pdf/articles`, the default bucket name is `jsmith7.pdf.artcles`. Alternatively, you can run `dat init <bucket>` and specify your own bucket name.

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

## To do

* Would be nice to have a proper install script
* Limited testing by users other than me
* Would be nice to include more `git`-like tools: `.datignore`, `dat reset` and so on to roll back accidental changes, etc.
* Currently specific to AWS, would be neat if it worked with, say, Google Drive as well.
