from yunobuiltin import if_let, get, assoc, is_iterable
from .utils import merge, escape_query_string_characters


# going to shadow some builtins
builtin_range = range
builtin_bool = bool
builtin_filter = filter

# query and filter take single expressions. to make them take multiple
# expressions use and/or or bool
# query(term('foo','bar')) => {'query': {'term': {'foo': 'bar'}}}
query = lambda q: {"query": q}
# filter(term('foo','bar')) => {'filter': {'term': {'foo': 'bar'}}}
filter = lambda q: {"filter": q}

size = lambda s: {"size": s}
limit = size
from_ = lambda f: {"from": f}
offset = from_
sort = lambda *fields: {"sort": fields}

def term(key, values, **options):
    op = 'term'
    if is_iterable(values) and not isinstance(values, basestring):
        op = 'terms'

    if op == 'terms':
        return {op: merge({key: values}, options)}
    else:
        return {op: {key: merge({"value": values}, options)}}


range = lambda k, **opts: {"range": {k: opts}}
match = lambda f, q, **opts: {"match": {f: merge({"query": q}, opts)}}
bool = lambda **opts: {"bool": opts}
boosting = lambda **opts: {"boosting": opts}
ids = lambda type, ids: {"ids": {"type": type, "values": ids}}
constant_score = lambda **opts: {"constant_score": opts}
dis_max = lambda **opts: {"dis_max": opts}
prefix = lambda **opts: {"prefix": opts}
filtered = lambda **opts: {"filtered": opts}
flt = fuzzy_like_this = lambda **opts: {"fuzzy_like_this": opts}
flt_field = fuzzy_like_this_field = lambda **opts: {"fuzzy_like_this_field": opts}
fuzzy = lambda **opts: {"fuzzy": opts}
match_all = lambda **opts: {"match_all": opts if opts else {}}
multi_match = lambda **opts: {"multi_match": opts}
mlt = more_like_this = lambda **opts: {"more_like_this": opts}
mlt_field = more_like_this_field = lambda **opts: {"more_like_this_field": opts}


def query_string(escape_fn=None, **options):
    if escape_fn is None:
        escape_fn = escape_query_string_characters

    options = if_let(get(options, "query"),
                     lambda q: assoc(options, "query", escape_fn(q)),
                     options)

    return {"query_string": options}

query_string_unescaped = lambda **opts: {'query_string': opts}
simple_query_string = lambda **opts: {'simple_query_string': opts}


span_first = lambda **opts: {"span_first": opts}
span_near = lambda **opts: {"span_near": opts}
span_not = lambda **opts: {"span_not": opts}
span_or = lambda **opts: {"span_or": opts}
span_term = lambda **opts: {"span_term": opts}
wildcard = lambda **opts: {"wildcard": opts}
indices = lambda **opts: {"indices": opts}
has_child = lambda **opts: {"has_child": opts}
custom_filters_score = lambda **opts: {"custom_filters_score": opts}
function_score = lambda **opts: {"function_score": opts}
top_children = lambda **opts: {"top_children": opts}
nested = lambda **opts: {"nested": opts}
common = lambda **opts: {"common": opts}
exists = lambda f: {"exists": {"field": f}}
