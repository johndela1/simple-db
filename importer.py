#!/usr/bin/env python

import argparse
from contextlib import contextmanager
from datetime import datetime
import fileinput
from pathlib import Path
import _pickle as pickle
import struct
import sys


COL_FMT = '64s 64s 64s I e H 56x'
COL_NAMES = ['STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']
REC_SIZE = struct.calcsize(COL_FMT)

@contextmanager
def use_index(fname):
    try:
        with open(fname, 'rb') as index_file:
            index = pickle.load(index_file)
    except FileNotFoundError:
        index = {}
    yield index
    with open(fname, 'wb') as index_file:
        pickle.dump(index, index_file)

def serialize(rec):
    def tr_str(s):
        return [bytes(e, 'utf-8') for e in s]
    def tr_date(s):
        year, month, day = [int(s) for s in s.split('-')]
        return year << 9 | month << 5 | day
    def tr_rev(s):
        return float(s)
    def tr_view_time(s):
        hours, minutes = [int(e) for e in s.split(':')]
        return hours * 60 + minutes
    cols = (
        *tr_str([rec['STB'], rec['TITLE'], rec['PROVIDER']]),
        tr_date(rec['DATE']),
        tr_rev(rec['REV']),
        tr_view_time(rec['VIEW_TIME']),
    )
    return struct.pack(COL_FMT, *cols)

def deserialize(rec):
    def tr_str(t):
        return [b.decode('utf-8').rstrip('\0') for b in t]
    def tr_date(i):
        year = i >> 0x9
        month = (i >> 0x5) & 0xf
        day = i & 0x1f
        return datetime(year, month, day)
    def tr_rev(i):
        return float(i)
    def tr_view_time(s):
        hours, minutes = divmod(int(s), 60)
        return ':'.join([str(i) for i in [hours, minutes]])
    rec = dict(zip(COL_NAMES, struct.unpack(COL_FMT, rec)))
    return [
        *tr_str([rec['STB'], rec['TITLE'], rec['PROVIDER']]),
        tr_date(rec['DATE']),
        tr_rev(rec['REV']),
        tr_view_time(rec['VIEW_TIME']),
    ]

def dump_and_exit(db_file):
    with open(db_file, 'rb') as f:
        while True:
            buf = f.read(REC_SIZE)
            if not buf:
                break
            print(deserialize(buf))
    exit()

def upsert(db_file, index_ref, rec):
    key = rec['STB'], rec['TITLE'], rec['DATE']
    value = serialize(rec)
    pos = index_ref.get(key)
    if pos is not None:
        db_file.seek(pos)
        db_file.write(value)
    else:
        db_file.seek(0,2) #seek to end
        pos = db_file.tell()
        db_file.write(value)
        index_ref[key] = pos

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='read standard input')
    parser.add_argument('name', help='name of output database')
    parser.add_argument('-d', '--dump', action='store_true', help='dump database')
    args = parser.parse_args()
    db_fname = args.name+'.bin'
    index_fname = args.name+'.idx'
    Path(db_fname).touch()

    if args.dump:
        dump_and_exit(db_fname)

    with open(db_fname, 'r+b') as db_file, use_index(index_fname) as index_ref:
        for line in fileinput.input('-'):
            cols = line.strip().split('|')
            if fileinput.isfirstline():
                col_names = cols
                assert(col_names == COL_NAMES)
                continue
            rec = dict(zip(col_names, cols))
            upsert(db_file, index_ref, rec)

# Records in the datastore should be unique by STB, TITLE and DATE.
# Subsequent imports with the same logical record should overwrite
# the earlier records.
#
# ./importer db <<EOF
# STB|TITLE|PROVIDER|DATE|REV|VIEW_TIME
# stb1|the matrix|warner bros|2014-04-01|4.00|1:30
# stb1|unbreakable|buena vista|2014-04-03|6.00|2:05
# stb2|the hobbit|warner bros|2014-04-02|8.00|2:45
# stb3|the matrix|warner bros|2014-04-02|4.00|1:05
# EOF
# ./importer db --dump
