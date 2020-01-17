#!/usr/bin/env python
#
# dubug - [du] [b]y [u]ser and [g]roup
#
# Simple utility to walk a directory hierarchy and record per-user
# and per-group byte usage.
#

import os, sys, operator, argparse
from stat import *

# Initialize our
total_usage = 0
per_user_usage = {}
per_group_usage = {}

def to_human_readable(bytes):
    units = [ 'B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB' ]
    unit = 0
    while ( bytes > 1024.0 and unit < len(units) ):
        unit = unit + 1
        bytes = bytes / 1024.0
    return '{:.2f} {:s}'.format(bytes, units[unit])

#
# Get arguments parsed:
#
cli_parser = argparse.ArgumentParser(description='Summary disk usage by user and group')
cli_parser.add_argument('rootdir',
        nargs=1,
        help='Scan usage starting at this directory'
    )
cli_parser.add_argument('--verbose', '-v',
        dest='verbose',
        action='store_true',
        default=False,
        help='increase the amount of information printed'
    )
cli_parser.add_argument('--human-readable', '-H',
        dest='is_human_readable',
        action='store_true',
        default=False,
        help='output usage levels with units (rather than just in bytes, period)'
    )
cli_args = cli_parser.parse_args()

#
# Validate the rootdir:
#
if len(cli_args.rootdir) <> 1:
    sys.stderr.write('ERROR:  zero or more than one rootdir provided...?\n')
    sys.exit(1)
cli_args.rootdir = cli_args.rootdir[0]
try:
    finfo = os.stat(cli_args.rootdir)
    if not S_ISDIR(finfo.st_mode):
        sys.stderr.write('ERROR:  {:s} is not a directory\n'.format(cli_args.rootdir))
        exit(1)
except:
    sys.stderr.write('ERROR:  {:s} does not exist or you do not have privileges to see it\n'.format(cli_args.rootdir))
    sys.exit(1)

#
# Walk the directory hierarchy; probably slow with Python 2.7, but should be
# faster with Python 3.5 or newer.
#
for root, dir, files in os.walk(cli_args.rootdir):
    # Make sure to account for the directory itself:
    try:
        finfo = os.stat(root)
        if cli_args.verbose:
            print root
        if finfo.st_uid not in per_user_usage:
            per_user_usage[finfo.st_uid] = finfo.st_size
        else:
            per_user_usage[finfo.st_uid] = per_user_usage[finfo.st_uid] + finfo.st_size
        if finfo.st_gid not in per_group_usage:
            per_group_usage[finfo.st_gid] = finfo.st_size
        else:
            per_group_usage[finfo.st_gid] = per_group_usage[finfo.st_gid] + finfo.st_size
        total_usage = total_usage + finfo.st_size
    except:
        pass

    # No need to look at the dir list, they'll be the next set of root values.

    # Explore the files in the directory:
    for file in files:
        try:
            finfo = os.stat(os.path.join(root, file))
            if finfo.st_uid not in per_user_usage:
                per_user_usage[finfo.st_uid] = finfo.st_size
            else:
                per_user_usage[finfo.st_uid] = per_user_usage[finfo.st_uid] + finfo.st_size
            if finfo.st_gid not in per_group_usage:
                per_group_usage[finfo.st_gid] = finfo.st_size
            else:
                per_group_usage[finfo.st_gid] = per_group_usage[finfo.st_gid] + finfo.st_size
            total_usage = total_usage + finfo.st_size
        except:
            pass

#
# Create a list of key-value tuples sorted by value (byte usage):
#
sorted_per_user = sorted(per_user_usage.items(), key=operator.itemgetter(1))
sorted_per_group = sorted(per_group_usage.items(), key=operator.itemgetter(1))

#
# Output the summary:
#
if cli_args.is_human_readable:
    print '{:20s} {:>24s}'.format('Total usage', to_human_readable(total_usage))
    print '{:20s}'.format('Per-user usage')
    for uid, bytes in sorted_per_user:
        print '  {:>18d} {:>24s}'.format(uid, to_human_readable(bytes))
    print '{:20s}'.format('Per-group usage')
    for gid, bytes in sorted_per_group:
        print '  {:>18d} {:>24s}'.format(gid, to_human_readable(bytes))
else:
    print '{:20s} {:>24d}'.format('Total usage', total_usage)
    print '{:20s}'.format('Per-user usage')
    for uid, bytes in sorted_per_user:
        print '  {:>18d} {:>24d}'.format(uid, bytes)
    print '{:20s}'.format('Per-group usage')
    for gid, bytes in sorted_per_group:
        print '  {:>18d} {:>24d}'.format(gid, bytes)

