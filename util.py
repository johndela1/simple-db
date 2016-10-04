from datetime import datetime
from operator import and_, or_
import pyparsing as pp

COL_NAMES = ['STB', 'TITLE', 'PROVIDER', 'DATE', 'REV', 'VIEW_TIME']
COL_FMT = '64s 64s 64s I H H 56x'

def nested(seq):
    return any(True for i in seq if type(i) == pp.ParseResults)

def eval_(expr):
    x, op, y = expr

    if op == 'AND':
        f = and_
    else:
        f = or_

    if op == '=':
        return x == y
    if not nested(x) and not nested(y):
        return f(x[0] == x[2], y[0] == y[2])
    if not nested(x) and nested(y):
        return f(x[0] == x[2], eval_(y))
    if nested(x) and not nested(y):
        return f(eval_(x), y[0] == y[2])


def col_num(column):
    if column is None:
        return None
    if column:
        return COL_NAMES.index(column)


def col_nums(columns):
    if columns is None:
         return None
    return [COL_NAMES.index(col) for col in columns if col]


def parse(expr_string, row):
    def fill_in(tree, tree_types=(pp.ParseResults)):
        if not isinstance(tree, tree_types):
            return []
        op = tree[1]
        if op == '=':
            new_col_val = row[col_num(tree[0])]
            tree[0] = new_col_val
            return []
        for node in tree:
            for subvalue in fill_in(node, tree_types):
                return []
        return []


    def to_date(s, loc, tokens):
        y, m, d = (int(i) for i in tokens[0].split('-'))
        return datetime(y, m, d)
    date = pp.Regex(r'\d{4}[-/]\d{2}[-/]\d{2}')
    date.setParseAction(to_date)

    real = pp.Combine(
            pp.Word(pp.nums) + "." + pp.Optional(
                pp.Word(pp.nums))).setName("real")
    real = pp.Regex(r'\d+\.\d+')

    real.setParseAction(lambda s, l, t: float(t[0]))

    operator = pp.Regex('=').setName('operator')
    number = pp.Regex(r'\d+(:?\.\d*)?(:?[eE][+-]?\d+)?')
    number = pp.Word(pp.nums)
    identifier = pp.Word(pp.srange("[a-zA-Z]"))
    parser = pp.QuotedString(quoteChar = '"')
    comparison_term = real | date | identifier | number | parser
    condition = pp.Group(comparison_term + operator + comparison_term)

    expr = pp.operatorPrecedence(condition,[
        ('AND', 2, pp.opAssoc.LEFT, ),
        ('OR', 2, pp.opAssoc.LEFT, ),
        ]
        )
    res = expr.parseString(expr_string)[0]
    fill_in(res)
    return res
