import argparse
import fileinput
from ctypes import c_char, c_int
from ctypes import Structure
import struct

class Fmt(Structure):
    _fields_ = [('stb', c_char * 64), ('title', c_char *64)]

#stb, title, provider, date, rev, view_time
FMT = '64s 64s 64s h b b h h 56x'


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

parser = argparse.ArgumentParser()
parser.add_argument("infiles", nargs="*", default='-')
args = parser.parse_args()

fmt = Fmt()

with open('data.db', 'wb') as f: #, open('data.idx','wb') as idx:
    for line in fileinput.input(args.infiles):
        if fileinput.isfirstline():
            header = line
            print('header', header)
            continue
        rec = serialize(line.split('|'))
        f.write(rec)

exit()
        # for f in struct.unpack(FMT, rec):
        #     if type(f) == bytes:
        #         print(f.decode('utf8').rstrip('\0'))
        #     else:
        #         print(f)
