import os
import json
import re

from elasticsearch import Elasticsearch, helpers
import xmltool

from waxe.core import browser

import logging
log = logging.getLogger(__name__)


HITS_PER_PAGE = 20
COMPLETION_HITS = 20

DOC_TYPE_FILE = 'file'
DOC_TYPE_TAG = 'tag'


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
            "_default_": {
                "properties": {
                    "abspath": {
                        "type": "string",
                        "analyzer": "path_analyzer"
                    },
                    "relpath": {
                        "type": "string",
                        "analyzer": "path_analyzer"
                    },
                    "path_completion": {
                        "type": "string",
                        "index_analyzer": "nGram_analyzer",
                        "search_analyzer": "whitespace_analyzer"
                    },
                    "extension": {
                        "index": "not_analyzed",
                        "type": "string"
                    },
                    "time": {
                        "type": "float"
                    },
                }
            }
        }
    }
    client.indices.create(index=index, body=body)


def index_file(client, index, path, root_path):
    """Index a file in elasticsearch
    """
    filename, ext = os.path.splitext(path)
    log.debug('Indexing path %s' % path)
    content = None
    try:
        obj = xmltool.load(path)
        with open(path, 'r') as f:
            content = f.read()

        # Remove the XML tags
        content = re.sub(r'\s*<[^>/]*?>', ' ', content)
        content = re.sub(r'<[^>]*?>\s*', ' ', content)
    except:
        log.exception("Can't load %s." % path)
        return

    relpath = browser.relative_path(path, root_path)
    body = {
        'abspath': path,
        'relpath': relpath,
        'extension': ext,
        'path_completion': relpath.replace('/', ' '),
        "content": content,
        'time': os.path.getmtime(path),
    }
    res = client.index(index=index, doc_type=DOC_TYPE_FILE, body=body)

    tags = {}
    for elt in obj.walk():
        if not isinstance(elt, xmltool.elements.TextElement):
            continue
        if not elt.text:
            continue
        tags.setdefault(elt.tagname, []).append(elt.text)

    for k, v in tags.iteritems():
        body = {
            'abspath': path,
            'relpath': relpath,
            'extension': ext,
            'tag': k,
            # Don't use contents to have the same field names like in
            # DOC_TYPE_FILE
            'content': v
        }
        res = client.index(index=index, doc_type=DOC_TYPE_TAG, body=body)

        if not res['created']:
            log.error('Error indexing file (tag) %s in %s.%s.\n\n%s' % (
                path, index, DOC_TYPE_TAG, res))

    log.debug('Indexed path %s' % path)


def delete_file(client, index, ident, abspath):
    log.debug('deleting file %s in %s.%s' % (abspath, index, DOC_TYPE_FILE))

    body = {
        "query": {
            "term": {
                "abspath": abspath
            }
        }
    }
    ok = True
    rows = helpers.scan(client, index=index, doc_type=DOC_TYPE_TAG, query=body)
    for row in rows:
        tag_ident = row['_id']
        response = client.delete(index=index, doc_type=DOC_TYPE_TAG,
                                 id=tag_ident)
        if not response['found']:
            ok = False
            log.error('Error deleting file %s in %s.%s.\n\n%s' % (
                abspath, index, DOC_TYPE_TAG, response))

    if not ok:
        return

    # Delete the entry in file only if all the entries in tag are deleted. It's
    # very important since the use entry in file to know if a file exists. If
    # there is a failure while deleting tag, we need the entry in file for the
    # next scan.
    response = client.delete(index=index, doc_type=DOC_TYPE_FILE, id=ident)
    if not response['found']:
        log.error('Error deleting file %s in %s.%s.\n\n%s' % (
            abspath, index, DOC_TYPE_FILE, response))
    log.debug('deleted file %s in %s.%s' % (abspath, index, DOC_TYPE_FILE))


def incremental_index(url, index, paths, root_path, force=False):
    client = Elasticsearch(url)

    # Get all the files for reindexing
    rows = helpers.scan(client, index=index, doc_type=DOC_TYPE_FILE)

    done = []

    for row in rows:
        path = row['_source']['abspath']
        ident = row['_id']

        if not force and path not in paths:
            # Partial update
            continue

        done += [path]

        if not os.path.exists(path):
            log.debug("%s dosn't exist" % path)
            delete_file(client, index, ident, path)
            continue

        indexed_time = row['_source']['time']
        mtime = os.path.getmtime(path)
        if mtime > indexed_time:
            # TODO: find a better way to update document
            # The file has changed, we delete it to reindex it!
            log.debug('%s has changed' % path)
            delete_file(client, index, ident, path)
            index_file(client, index, path, root_path)

    for path in paths:
        if path not in done:
            index_file(client, index, path, root_path)


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

    if tag:
        conditions.append({
            'term': {
                'tag': tag
            }
        })

    query = {
        'match': {
            'content': expr
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

    body['highlight'] = {
        "pre_tags": ["<strong>"],
        "post_tags": ["</strong>"],
        "fields": {
            "content": {}
        }
    }
    doc_type = DOC_TYPE_FILE
    if tag:
        doc_type = DOC_TYPE_TAG

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
