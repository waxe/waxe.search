#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from waxe.search import elastic
from elasticsearch import Elasticsearch, helpers


class TestElastic(unittest.TestCase):

    def test_completion_path(self):
        url = 'http://127.0.0.1:9200'
        index = 'waxe-test'
        client = Elasticsearch(url)
        client.indices.delete(index, ignore=404)
        elastic._init_settings(client, index)

        relpath = 'GFGS/V2/VOCAB/A/APERCEVOIR-PASSIF-PASSECOMPOSE-3PS.xml'
        body = {
            'relpath': relpath,
            'path_completion': relpath,
        }
        res = client.index(index=index, doc_type=elastic.DOC_TYPE_FILE, body=body)
        # Make sure elastic has indexed.
        import time
        time.sleep(1)

        value = 'GFGS'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'GFGS APE'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'APE GFGS'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'GFGS VOC 3PS'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'VO SE'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'GFGS/V2/VOCAB'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'GFGS V2 VOCAB A APERCEVOIR'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = 'GFGS V2 VOCAB A APERCEVOIR PASSIF PASSECOMPOSE 3PS xml'
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])

        value = relpath
        res = elastic.path_completion(url, index, value)
        self.assertEqual(res, [relpath])
