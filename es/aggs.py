from .utils import merge

## Aggregation Functions

# going to be shadowing some builtins
builtin_min = min
builtin_max = max
builtin_sum = sum
builtin_filter = filter

# build a top-level aggs fn
aggs = lambda **fns: {"aggs": fns}

min = lambda f: {"min": {"field": f}}
max = lambda f: {"max": {"field": f}}
sum = lambda f: {"sum": {"field": f}}
avg = lambda f: {"avg": {"field": f}}
stats = lambda f: {"stats": {"field": f}}
extended_stats = lambda f: {"extended_stats": {"field": f}}
value_count = lambda f: {"value_count": {"field": f}}
percentiles = lambda f: {"percentiles": {"field": f}}


def cardinality(field, opts=None):
    if opts is None:
        opts = {}
    return {"cardinality": merge({"field": field}, opts)}


filter = lambda opts: {"filter": opts}
missing = lambda f: {"missing": {"field": f}}
nested = lambda opts: {"nested": opts}


def terms(field, opts=None):
    if opts is None:
        opts = {}
    return {"terms": merge({"field": field}, opts)}


def range(field, ranges, opts=None):
    if opts is None:
        opts = {}
    return {"range": merge({"field": field,
                            "ranges": ranges},
                           opts)}


date_range = lambda f, format, ranges: {"date_range": {"field": f,
                                                       "ranges": ranges,
                                                       "format": format}}
ip_range = lambda f, ranges: {"ip_range": {"field": f, "ranges": ranges}}


def histogram(field, interval, opts=None):
    if opts is None:
        opts = {}
    return {"histogram": merge({"field": field,
                                "interval": interval},
                               opts)}


def date_histogram(field, interval, opts=None):
    if opts is None:
        opts = {}
    return {"date_histogram": merge({"field": field,
                                     "interval": interval},
                                    opts)}
