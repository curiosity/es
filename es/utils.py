import logging
import re
from functools import partial
from yunobuiltin import merge_with, thread, get_in

logger = logging.getLogger(__name__)

merge = merge_with = partial(merge_with, lambda x, y: x)

# Characters that are part of Lucene query syntax must be stripped
# from user input: + - && || ! ( ) { } [ ] ^ " ~ * ? : \ /
# See: http://lucene.apache.org/core/4_7_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
ESCAPE_CHAR_RE = r'([*\-+!(){}\[\]^\"~?:\\\/])'
ESCAPE_CHARS_RE = r'(&&|\|\|)'


def escape_query_string_characters(text):
    """
    Remove Lucene reserved characters from query string
    """
    return thread(text,
                  lambda t: re.sub(ESCAPE_CHAR_RE, r'\\\1', t),
                  lambda t: re.sub(ESCAPE_CHARS_RE, r'\\\1', t))


def unique_index_name(name):
    """ Generates a unique name based on name """
    from uuid import uuid4
    return '-'.join((name, str(uuid4())[:8]))


def migrate_index(es, index_name, doc_type, mapping, func=None):
    """ Migrates index_name to a new mapping, creating an alias if necessary

        Takes an optional func with the signature:

            def func(es, old_index_name, new_index_name):

        This will be ran after reindexing but prior to old index deletion or
        aliasing.

        Returns True if everything went okay.
    """
    from .core import (create_index,
                       index_exists,
                       get_alias,
                       put_settings,
                       put_mapping,
                       alias_exists,
                       delete_index,
                       put_alias,
                       delete_alias, )
    lock_index_name = "_".join((index_name, "migration"))
    try:
        logger.warn('Acquiring %s lock.', lock_index_name)
        create_index(es, lock_index_name)
    except Exception:
        logger.warn("Bailing on %s because lock could not be acquired.", lock_index_name)
        return False
    try:
        from elasticsearch.helpers import reindex
        old_indexes = []
        if index_exists(es, index_name):
            old_indexes = get_alias(es, index_name).keys()
            for i in old_indexes:
                put_settings(es, i, {'index.blocks.write': True})

        new_index_name = unique_index_name(index_name)
        create_index(es, new_index_name)
        put_mapping(es, new_index_name, doc_type, mapping)

        if index_exists(es, index_name):
            reindex(client=es, source_index=index_name, target_index=new_index_name)
        if callable(func):
            func(es, index_name, new_index_name)

        if not alias_exists(es, index_name) and index_exists(es, index_name):
            delete_index(es, index_name)
        put_alias(es, index_name, new_index_name)
        for i in old_indexes:
            delete_alias(es, index_name, i)
            delete_index(es, i)
        return True
    except Exception, e:
        logger.warn(str(e))
    finally:
        try:
            logger.debug('Cleaning up %s lock.', lock_index_name)
            delete_index(es, lock_index_name)
        except Exception, e:
            logger.warn(str(e))


def does_need_migration(es, index_name, doc_type, mapping):
    """ True if provided mapping is different from the deployed mapping. """
    from es.core import index_exists, get_mapping
    if index_exists(es, index_name):
        return not (get_in(get_mapping(es, index_name), [index_name, doc_type]) == mapping)
    return True


def move_alias(es, alias_name, index_name, drop=None):
    """
    Move alias_name to index_name (removing other references). If drop=True,
    also delete the other indexes.
    """
    from .core import get_alias, put_alias, delete_alias, delete_index, index_exists

    if index_exists(es, alias_name):
        old_indexes = [x for x in get_alias(es, alias_name).keys() if x != index_name]
    else:
        old_indexes = []

    logger.info('Adding new index %s to alias %s', index_name, alias_name)
    put_alias(es, alias_name, index_name)

    for i in old_indexes:
        logger.info('Removing index %s from alias %s', alias_name, i)
        delete_alias(es, alias_name, i)
        if drop:
            logger.info('Deleting index %s', i)
            delete_index(es, i)


def parameterized_reindex(client, source_index, target_index, target_client=None, chunk_size=500, scroll='5m', query=None):
    """
    Reindex all documents from one index to another, potentially (if
    `target_client` is specified) on a different cluster.

    .. note::

        This helper doesn't transfer mappings, just the data.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use (for
        read if `target_client` is specified as well)
    :arg source_index: index (or list of indices) to read documents from
    :arg target_index: name of the index in the target cluster to populate
    :arg target_client: optional, is specified will be used for writing (thus
        enabling reindex between clusters)
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg query: optional, is body for the :meth:`~elasticsearch.Elasticsearch.search` api
    """
    from elasticsearch.helpers import scan, bulk

    target_client = client if target_client is None else target_client

    docs = scan(client, query, scroll, index=source_index)

    def _change_doc_index(hits, index):
        for h in hits:
            h['_index'] = index
            yield h

    return bulk(target_client, _change_doc_index(docs, target_index),
                chunk_size=chunk_size, stats_only=True)


def sync(source_es,
         target_es,
         index_name,
         unique=True,
         mappings=None,
         move_aliasp=False,
         chunk_size=500,
         scroll='30m',
         query=None):
    from .core import create_index

    if mappings is None:
        mappings = {}

    if unique:
        name = unique_index_name(index_name)
    else:
        name = index_name

    logger.info('Creating index %s', name)
    create_index(target_es, name, mappings)

    logger.info('Starting reindex...')
    parameterized_reindex(client=source_es,
                          source_index=index_name,
                          target_index=name,
                          target_client=target_es,
                          chunk_size=chunk_size,
                          scroll=scroll,
                          query=query)
    logger.info('Reindex finished!')

    if unique and move_aliasp:
        logger.info('Moving alias')
        move_alias(target_es, index_name, name, drop=True)
