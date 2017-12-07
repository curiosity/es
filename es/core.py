import logging
from .utils import merge

logger = logging.getLogger(__name__)


def get_conn(url, **kwargs):
    from elasticsearch import Elasticsearch
    return Elasticsearch(url, **kwargs)


def search(es, index, query, **kwargs):
    return es.search(**merge({"index": index,
                              "body": query},
                             kwargs))


def index(es, index, doc_type, doc, **kwargs):
    return es.index(**merge(dict(index=index,
                                 doc_type=doc_type,
                                 body=doc),
                            kwargs))


def delete_index(es, name):
    """ Deletes the index """
    return es.indices.delete(name)


def create_index(es, name, body=None):
    """ Creates the index """
    return es.indices.create(name, body=body or {})


def index_exists(es, name):
    """ True if the index exists (possibly as an alias) """
    return es.indices.exists(name)


def put_alias(es, name, index):
    """ Puts an index in the named alias """
    return es.indices.put_alias(name=name, index=index)


def delete_alias(es, name, index):
    """ Deletes the index from the named alias"""
    return es.indices.delete_alias(name=name, index=index)


def alias_exists(es, name):
    """ True if the name exists as an alias """
    return es.indices.exists_alias(name)


def get_alias(es, name):
    """ Gets an alias """
    return es.indices.get_alias(index=name)


def put_settings(es, name, settings):
    """ Puts some settings into an index """
    return es.indices.put_settings(index=name, body=settings)


def block_writes(es, name, value=True):
    """ Blocks writes to an index """
    return put_settings(es, name, {'index.blocks.write': value})


def unblock_writes(es, name, value=False):
    """ Unblocks writes to an index """
    return block_writes(es, name, value=not value)


def put_mapping(es, name, doc_type, mapping):
    """ Puts a mapping to an index at a doc_type """
    return es.indices.put_mapping(index=name,
                                  doc_type=doc_type,
                                  body={doc_type: mapping})


def get_mapping(es, name):
    """ Returns the mapping """
    return es.indices.get_mapping(name)
