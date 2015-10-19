import os
import hashlib

from elasticsearch import Elasticsearch, helpers
import xmltool

# TODO: not really nice, but since we always want cache on the dtd...
import xmltool.cache
xmltool.cache.CACHE_TIMEOUT = 3600


from waxe.core import browser

import logging
log = logging.getLogger(__name__)


HITS_PER_PAGE = 20
COMPLETION_HITS = 20

DOC_TYPE_FILE = 'waxe-file'


def _init_settings(url, index):
    """Create the index if it not exists
    """
    client = Elasticsearch(url)
    if client.indices.exists(index=index):
        return

    body = {
        "settings": {
            "analysis": {
                "filter": {
                    "nGram_filter": {
                        "type": "nGram",
                        "min_gram": 2,
                        "max_gram": 20,
                        "token_chars": [
                            "letter",
                            "digit",
                            "punctuation",
                            "symbol"]
                    }
                },
                "analyzer": {
                    "nGram_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "asciifolding",
                            "nGram_filter"
                        ]
                    },
                    "whitespace_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "asciifolding"
                        ]
                    },
                    "path_analyzer": {
                        "type": "custom",
                        "tokenizer": "path_hierarchy"
                    },
                    "html_analyzer": {
                        "type": "custom",
                        "char_filter":  ["html_strip"],
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "asciifolding"
                        ]
                    }
                }
            },
        },
        "mappings": {
            DOC_TYPE_FILE: {
                "date_detection": False,
                "properties": {
                    "abspath": {
                        "type": "string",
                        "analyzer": "path_analyzer",
                        "include_in_all": False
                    },
                    "relpath": {
                        "type": "string",
                        "analyzer": "path_analyzer",
                        "include_in_all": False
                    },
                    "extension": {
                        "index": "not_analyzed",
                        "type": "string",
                        "include_in_all": False
                    },
                    "path_completion": {
                        "type": "string",
                        "index_analyzer": "nGram_analyzer",
                        "search_analyzer": "whitespace_analyzer",
                        "include_in_all": False
                    },
                    "time": {
                        "type": "float",
                        "include_in_all": False
                    },
                }
            }
        }
    }
    client.indices.create(index=index, body=body)


def index_file(client, index, ident, path, root_path):
    filename, ext = os.path.splitext(path)
    relpath = browser.relative_path(path, root_path)
    try:
        obj = xmltool.load(path)
    except:
        log.exception("Can't load %s for indexing" % path)
        return

    tags = {}
    attrs = []

    # In waiting xmltool include self in the walk, do it manually
    def parse_elt(elt):
        for kv in (elt.attributes or {}).iteritems():
            attrs.append('%s=%s' % kv)
        if not isinstance(elt, xmltool.elements.TextElement):
            return
        if not elt.text:
            return
        tags.setdefault(elt.tagname, []).append(elt.text)

    parse_elt(obj)
    for elt in obj.walk():
        parse_elt(elt)

    body = {
        'abspath': path,
        'relpath': relpath,
        'extension': ext,
        'path_completion': relpath.replace('/', ' '),
        'time': os.path.getmtime(path),
        "tags": tags,
        "attrs": attrs
    }
    client.index(index=index, doc_type=DOC_TYPE_FILE, body=body, id=ident)


def delete_file(client, index, ident, abspath):
    response = client.delete(index=index, doc_type=DOC_TYPE_FILE, id=ident)
    if not response['found']:
        log.error('Error deleting file %s in %s.%s.\n\n%s' % (
            abspath, index, DOC_TYPE_FILE, response))


def partial_index(url, index, paths, root_path):
    client = Elasticsearch(url)
    for path in paths:
        ident = hashlib.sha224(path).hexdigest()
        if os.path.exists(path):
            index_file(client, index, ident, path, root_path)
        else:
            delete_file(client, index, ident, path)


def incremental_index(url, index, paths, root_path):
    try:
        client = Elasticsearch(url)
        # Get all the files for reindexing
        rows = helpers.scan(client, index=index, doc_type=DOC_TYPE_FILE)
        done = []
        for row in rows:
            path = row['_source']['abspath']
            ident = row['_id']

            done += [path]

            if not os.path.exists(path):
                log.debug("%s dosn't exist" % path)
                delete_file(client, index, ident, path)
                continue

            indexed_time = row['_source']['time']
            mtime = os.path.getmtime(path)
            if mtime > indexed_time:
                log.debug('%s has changed' % path)
                index_file(client, index, ident, path, root_path)

        for path in paths:
            if path not in done:
                ident = hashlib.sha224(path).hexdigest()
                index_file(client, index, ident, path, root_path)

    except:
        log.exception('Failed to reindex')
        raise


def _search_body(expr, abspath, ext, tag):
    conditions = []
    if abspath:
        conditions.append({
            'prefix': {
                'abspath': abspath
            }
        })

    if ext:
        conditions.append({
            'term': {
                'extension': ext
            }
        })

    query = {
        'query_string': {
            'fields': [(tag or '_all')],
            "query": expr
        }
    }

    if not conditions:
        return {
            'query': query
        }

    if len(conditions) > 1:
        condition = {
            'bool': {
                'must': conditions
            }
        }
    else:
        condition = conditions[0]

    return {
        'query': {
            'filtered': {
                'filter': condition,
                'query': query,
            },
        }
    }


def do_search(url, index, expr, abspath=None, ext=None, tag=None, page=1):
    client = Elasticsearch(url)

    body = _search_body(expr, abspath, ext, tag)

    fields = 'tags.%s' % tag if tag else "*"
    body['highlight'] = {
        "pre_tags": ["<strong>"],
        "post_tags": ["</strong>"],
        "fields": {
            fields: {}
        }
    }
    doc_type = DOC_TYPE_FILE
    from_ = (page - 1) * HITS_PER_PAGE
    res = client.search(index=index, doc_type=doc_type, body=body,
                        size=HITS_PER_PAGE, from_=from_)
    return res['hits']['hits'], res['hits']['total']


def path_completion(url, index, value):
    client = Elasticsearch(url)
    body = {
        'query': {
            "match": {
                'path_completion': {
                    'query': value,
                    'operator': 'and'
                }
            }
        }
    }
    res = client.search(index=index, doc_type=DOC_TYPE_FILE, body=body,
                        size=COMPLETION_HITS)
    return [r['_source']['relpath'] for r in res['hits']['hits']]
