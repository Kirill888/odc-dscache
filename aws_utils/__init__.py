"""
"""


def slurp_lines(fname, *args, **kwargs):
    if len(args) > 0 or len(kwargs) > 0:
        fname = fname.format(*args, **kwargs)

    with open(fname, 'rt') as f:
        return [s.rstrip() for s in f.readlines()]
