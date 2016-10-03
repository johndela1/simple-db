#!/usr/bin/env python

import argparse
from datetime import datetime
from itertools import groupby
import struct
import sys
from mmap import mmap
from operator import itemgetter

from util import col_num, col_nums, COL_NAMES, COL_FMT, parse, eval_


def date(date_string): return datetime.strptime(date_string, '%Y-%m-%d')
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

    raw = struct.unpack(COL_FMT, rec)
    row = tr_str(raw[0:3]) + tr_date(raw[3]) + tr_rev(raw[4]) + [raw[5]]
    return row


def select(columns, table, filter_=None, group=None, order=None):
    rs = []
    for rec in table:
        row = deserialize(rec)
        if filter_ is None or where(row, filter_):
            rs.append(row)
    if group:
        group_col = col_num(group)
        agg_func = columns[4] #XXX change literal
        def group_func(x):
            x[group_col]

        return [(col, agg_func(rows))
                for col, rows
                in groupby(sorted(rs, key=group_func), group_func)]

    if order is not None:
        order_by_col_names = order.split(',')
        rs.sort(key=itemgetter(*col_nums(order_by_col_names)))

    col_getter = itemgetter(*(col_nums(columns)))
    return (col_getter(r) for r in rs)


def where(row, filter_expr):
    if filter_expr is None:
        return None
    tree = parse(filter_expr, row)
    return eval_(tree)


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
    except FileNotFoundError as e:
        print(e, file=sys.stdout)
