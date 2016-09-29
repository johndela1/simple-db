import argparse
import _pickle as pickle
import fileinput
from hashlib import sha1
from mmap import mmap
import struct


# stb, title, provider, date, rev, view_time
FMT = '64s 64s 64s I H H 56x'


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
    return struct.pack(FMT, *fields)


parser = argparse.ArgumentParser()
parser.add_argument("infiles", nargs="*", default='-')
args = parser.parse_args()

with open('data.db', 'w+b') as db_file:
    try:
        with open('data.idx', 'rb') as idx_file:
            idx = pickle.load(idx_file)
    except FileNotFoundError:
        print("init idx")
        idx = {}

    for line in fileinput.input(args.infiles):
        if fileinput.isfirstline():
            header = line
            # print('header', header)
            continue

        tokens = line.rstrip('\n').split('|')
        pk = (tokens[0]+tokens[1]+tokens[3]).encode()
        h = sha1(pk).digest()
        rec = serialize(line.split('|'))
        if h in idx.keys():
            end_pos = db_file.tell()
            db_file.seek(idx[h])
            db_file.write(rec)
            db_file.seek(end_pos)
            continue
        idx[h] = db_file.tell()
        db_file.write(rec)

    with open('data.idx', 'wb') as idx_file:
        pickle.dump(idx, idx_file)

# stb, title, provider, date, rev, view_time
# Subsequent imports with the same logical record should overwrite
# the earlier records.
# Records in the datastore should be unique by STB, TITLE and DATE.
