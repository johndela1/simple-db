import argparse
from ctypes import c_char, c_int
from ctypes import Structure
import struct
import mmap
import resource



FIELDS = ['STB','TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']

class Fmt(Structure):
    _fields_ = [('stb', c_char * 64), ('title', c_char *64)]

#stb, title, provider, date, rev, view_time
FMT = '64s 64s 64s h b b h h 56x'


def selected_cols(col_names):
    return [FIELDS.index(col) for col in col_names.split(',')]


def serialize(l):
    def tr_str(s):
        return [bytes(e, 'utf-8') for e in s]

    def tr_date(s):
        return [int(s) for s in s.split('-')]

    def tr_rev(s):
        return [int(float(s)*100)]

    def tr_view_time(s):
        hours, minutes = [int(e) for e in s.split(':')]
        return [hours * 60 + minutes]

    fields = tr_str(l[:3]) + tr_date(l[3]) + tr_rev(l[4]) + tr_view_time(l[5])
    return struct.pack(FMT, *fields)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--select')#, help='select statement')
    parser.add_argument('-o', '--order')#, help='order')
    parser.add_argument('-f', '--filter')
    args = parser.parse_args()

    if not args.select:
        print("must provide query")
        exit()

    PAGESIZE = resource.getpagesize()
    RECSIZE = 256
    BLKSIZE = PAGESIZE / RECSIZE
    cols = selected_cols(args.select)
    with open('data.db', 'r+b') as f, \
         open('data.idx','wb') as idx, \
         mmap.mmap(f.fileno(), 0) as m:
        foo = []
        for i in range(0, len(m), 256):
            print('---')
            rec = struct.unpack(FMT, m[i:i+256])
            res = []
            for c in cols:
                x=rec[c]
                if type(x) == bytes:
                    res.append(x.decode('utf8').rstrip('\0'))
                else:
                    res.append(x)
            foo.append(res)
        m.close()
    print(foo)
