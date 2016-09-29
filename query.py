import argparse
from datetime import datetime
import struct
from mmap import mmap


COL_NAMES = ['STB','TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']

# stb, title, provider, date, rev, view_time
FMT = '64s 64s 64s I H H 56x'


def deserialize(rec):
    def tr_str(t):
        return [b.decode('utf-8').rstrip('\0') for b in t]

    def tr_date(i):
        year = i >> 0x9
        month = i >> 0x5 & 0xf
        day = i & 0x1f
        return [datetime(year, month, day)]

    def tr_rev(i):
        return [float(i/100)]

    raw = struct.unpack(FMT, rec)

    row = tr_str(raw[0:3]) + tr_date(raw[3]) + tr_rev(raw[4]) + [raw[5]]
    return row


def select(col_names,from_ , where):
    rs = []
    for i in range(0, len(from_), RECSIZE):
        rec = from_[i:i+RECSIZE]
        row = deserialize(rec)
        if where is None or where(row):
            rs.append([row[name] for name in col_nums(col_names)])
    return rs


def col_nums(col_names):
    return [COL_NAMES.index(col) for col in col_names.split(',')]


def where(filt):
    if filt == None:
        return None
    col_name, val = filt.split('=')
    col_num = col_nums(col_name)[0]
    return lambda rec:rec[col_num] == val 


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--select', required=True)
    parser.add_argument('-o', '--order')
    parser.add_argument('-f', '--filter')
    args = parser.parse_args()

    RECSIZE = 256
    with open('data.db', 'r+b') as f, mmap(f.fileno(), 0) as from_:
        rs = select(args.select, from_, where(args.filter))
    print(rs, args.order)
