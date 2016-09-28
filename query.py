import argparse
import struct
from mmap import mmap


FIELDS = ['STB','TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']

# stb, title, provider, date, rev, view_time
FMT = '64s 64s 64s I H H 56x'


def deserialize(rec):
    def tr_str(t):
        return [b.decode('utf-8').rstrip('\0') for b in t]

    def tr_date(i):
        year = i >> 0x9
        month = i >> 0x5 & 0xf
        date = i & 0x1f
        return ['%.2d-%.2d-%.2d' % (year, month, date)]

    def tr_rev(i):
        return ["%.2f" % float(i/100)]

    def tr_view_time(i):
        hours = i // 60
        minutes = i % 60
        return ["%d:%.2d" % (hours, minutes)]

    raw = struct.unpack(FMT, rec)

    row = (tr_str(raw[0:3]) + tr_date(raw[3]) + tr_rev(raw[4]) +
           tr_view_time(raw[5]))
    return row


def select(cols, where, m):
    rs = []
    for i in range(0, len(m), 256):
        rec = m[i:i+256]
        row = deserialize(rec)
        if where(row):
            field_set = [row[i] for i in cols]
            rs.append(field_set)
    return rs


def cols(col_names):
    return [FIELDS.index(col) for col in col_names.split(',')]


def where(filt):
    return lambda rec: True


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--select')#, help='select statement')
    parser.add_argument('-o', '--order')#, help='order')
    parser.add_argument('-f', '--filter')
    args = parser.parse_args()

    if not args.select:
        print("must provide columns")
        exit()

    RECSIZE = 256
    with open('data.db', 'r+b') as f, mmap(f.fileno(), 0) as m:
        rs = select(cols(args.select), where(args.filter), m)
    print(rs)
