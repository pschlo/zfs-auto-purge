# zfsnap-prune
Policy-based purging of zfs snapshots. Uses "restic forget" syntax.

Also see "https://github.com/restic/restic/blob/master/internal/restic/snapshot_policy.go" and "https://restic.readthedocs.io/en/latest/060_forget.html"

## Installation
This package is not currently uploaded to PyPI. Install as follows:

1. Find your release of choice [here](https://github.com/pschlo/zfsnap-prune/releases)
2. Copy the link to `zfsnap_prune-x.x.x.tar.gz`
3. Run `python -m pip install {link}`

You may also prepend a [direct reference](https://peps.python.org/pep-0440/#direct-references), which might be desirable for a `requirements.txt`.


## Building
The `.tar.gz` file in a release is the [source distribution](https://packaging.python.org/en/latest/glossary/#term-Source-Distribution-or-sdist), which was created from the source code with `python3 -m build --sdist`. [Built distributions](https://packaging.python.org/en/latest/glossary/#term-Built-Distribution)
are not provided.
