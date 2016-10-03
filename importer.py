import argparse
from contextlib import contextmanager
import _pickle as pickle
import fileinput
from hashlib import sha1
from mmap import mmap
import struct
import sys

from util import COL_FMT


def serialize(l):
    def tr_str(s):
        return [bytes(e, 'utf-8') for e in s]

    def tr_date(s):
        year, month, day = [int(s) for s in s.split('-')]
        return [year << 9 | month << 5 | day]

    def tr_rev(s):
        return [int(float(s)*100)]

    def tr_view_time(s):
        hours, minutes = [int(e) for e in s.split(':')]
        return [hours * 60 + minutes]

    fields = tr_str(l[:3]) + tr_date(l[3]) + tr_rev(l[4]) + tr_view_time(l[5])
    return struct.pack(COL_FMT, *fields)


@contextmanager
def load_idx():
    try:
        with open('data.idx', 'rb') as idx_file:
            idx = pickle.load(idx_file)
    except FileNotFoundError:
        idx = {}
    yield idx
    with open('data.idx', 'wb') as idx_file:
        pickle.dump(idx, idx_file)


def key(row):
    cols = row.rstrip('\n').split('|')
    return sha1(
        (cols[0]+cols[1]+cols[3]).encode()
    ).digest()


def upsert(db_file, idx, row):

    rec = serialize(row.split('|'))
    k = key(row)
    pos = idx.get(k, None)

    if pos is not None:
        end_pos = db_file.tell()
        db_file.seek(pos)
        db_file.write(rec)
        db_file.seek(end_pos)
    else:
        idx[k] = db_file.tell()
        db_file.write(rec)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("infiles", nargs="*", default='-')
    args = parser.parse_args()

    with open('data.db', 'w+b') as db_file, load_idx() as idx:
        for row in fileinput.input(args.infiles):
            if fileinput.isfirstline():
                continue
            upsert(db_file, idx, row)

# stb, title, provider, date, rev, view_time
# Subsequent imports with the same logical record should overwrite
# the earlier records.
# Records in the datastore should be unique by STB, TITLE and DATE.
