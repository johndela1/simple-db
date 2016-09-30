import argparse
from datetime import datetime
from itertools import groupby, chain
import shlex
import struct
import sys
from mmap import mmap
from operator import itemgetter
import filt
import pyparsing as pp

def date(date_string): return datetime.strptime(date_string, '%Y-%m-%d')

COL_NAMES = ['STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']
FMT = '64s 64s 64s I H H 56x'
COL_TYPES = [str, str, str, date, float, int]


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


def select(columns, table, filter_=None, group=None, order=None):
    rs = []
    for rec in table:
        row = deserialize(rec)
        if where is None or where(row, filter_):
            rs.append(row)
    if group:
        cols = col_nums([group])
        if len(cols) > 1:
            raise ValueError("too many group column names")
        group_col = cols[0]
        agg_func = columns[4]
        group_func = lambda x: x[group_col]
        return [(col, agg_func(rows))
                for col, rows
                in groupby(sorted(rs, key=group_func), group_func)]

    if order is not None:
        order_by_col_names = order.split(',')
        rs.sort(key=itemgetter(*col_nums(order_by_col_names)))

    col_getter = itemgetter(*(col_nums(columns)))
    return (col_getter(r) for r in rs)


def col_nums(columns):
    if columns is None:
        return None
    return [COL_NAMES.index(col) for col in columns if col is not False]


def fill_in(tree, row, tree_types=(pp.ParseResults)):
    if not isinstance(tree, tree_types):
        return []
    #    import pdb;pdb.set_trace()
    op = tree[1]
    if op == '=':
        col_num = col_nums([tree[0]])[0]
        new_col_val = row[col_num]
        tree[0] = new_col_val 
        return []
    for node in tree:
        for subvalue in fill_in(node, row, tree_types):
            return []
    return []

def where(row, filter_):
    if filter_ is None:
        return None
    tree = filt.parse(filter_, row)
    x=fill_in(tree, row)
    #list(x)
     
    print("transformed tree", tree);exit()
    return filt.eval_(tree) # could be a lamba? like before


def from_(mm):
    for i in range(0, len(mm), RECSIZE):
        yield mm[i:i+RECSIZE]


agg_funcs = dict(
    MIN = min,
    MAX = max,
    SUM = sum,
    COUNT = lambda x: len(set(x)),
    COLLECT = lambda x: set(x),
)


def columns(names):
    ret = []
    col_names = names.split(',')
    stripped_col_names = [n.split(':')[0] for n in col_names]

    for col_name in COL_NAMES:
        if col_name not in stripped_col_names:
            ret.append(False)
            continue

        name_with_agg = col_names[stripped_col_names.index(col_name)]
        if ':' in name_with_agg:
            c, f = name_with_agg.split(':')
            agg_col_num = col_nums([c])[0]
            ret.append(
                lambda rows: agg_funcs[f.upper()](
                    [row[agg_col_num] for row in rows]))
        else:
            ret.append(col_name)
    return ret

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--select', required=True)
    parser.add_argument('-o', '--order')
    parser.add_argument('-f', '--filter')
    parser.add_argument('-g', '--group')
    return parser


if __name__ == '__main__':
    parser = parse_args(sys.argv[1:])
    args = parser.parse_args()
    RECSIZE = 256

    try:
        with open('data.db', 'r+b') as f, mmap(f.fileno(), 0) as table:
            rs = select(
                columns(args.select), 
                from_(table),
                args.filter,
                args.group,
                args.order,
            )
            for r in rs:
                print(r)
    except FileNotFoundError:
        pass
