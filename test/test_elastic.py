# -*- coding: utf-8 -*-

import eve
import time
import elasticsearch
from unittest import TestCase
from datetime import datetime
from copy import deepcopy
from flask import json
from eve.utils import config, ParsedRequest, parse_request
from eve_elastic.elastic import parse_date, Elastic, get_indices, get_es, generate_index_name
from nose.tools import raises
try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def highlight_callback(query_string):
    elastic_highlight_query = {
        'pre_tags': ['<span class=\"es-highlight\">'],
        'post_tags': ['</span>'],
        'fields': {
            'name': {'number_of_fragments': 0},
            'description': {'number_of_fragments': 0}
        }
    }

    if query_string:
        for key in elastic_highlight_query['fields']:
            # highlights do not use query_string anymore
            # refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-highlighting.html
            elastic_highlight_query['fields'][key]['highlight_query'] = query_string
        return elastic_highlight_query


DOMAIN = {
    'items': {
        'schema': {
            'uri': {'type': 'keyword'},
            'name': {'type': 'text'},
            'firstcreated': {'type': 'datetime', 'ignore_malformed': True},
            'category': {
                'type': 'keyword',
                'unique': True,
            },
            'dateline': {
                'type': 'dict',
                'schema': {
                    'place': {'type': 'text'},
                    'created': {'type': 'datetime'},
                    'extra': {'type': 'dict'},
                }
            },
            'place': {
                'type': 'list',
                'schema': {
                    'type': 'dict',
                    'schema': {
                        'name': {'type': 'text'},
                        'created': {'type': 'datetime'},
                    }
                }
            },
        },
        'datasource': {
            'backend': 'elastic',
            'projection': {'firstcreated': 1, 'name': 1},
            'default_sort': [('firstcreated', -1)]
        }
    },
    'published_items': {
        'schema': {
            'published': {'type': 'datetime'}
        },
        'datasource': {
            'source': 'items',
        },
    },
    'archived_items': {
        'schema': {'name': {'type': 'text'}, 'archived': {'type': 'datetime'}},
        'datasource': {'backend': 'elastic'},
    },
    'items_with_description': {
        'schema': {
            'uri': {'type': 'keyword'},
            'name': {'type': 'keyword'},
            'description': {'type': 'text'},
            'firstcreated': {'type': 'datetime', "ignore_unmapped" : True},
        },
        'datasource': {
            'backend': 'elastic',
            'projection': {'firstcreated': 1, 'name': 1},
            'default_sort': [('firstcreated', -1)],
            'elastic_filter': {'exists': {'field': 'description'}},
            'aggregations': {'type': {'terms': {'field': 'name'}}},
            'es_highlight': highlight_callback
        }
    },
    'items_with_callback_filter': {
        'schema': {
            'uri': {'type': 'text', 'unique': True}
        },
        'datasource': {
            'backend': 'elastic',
            'elastic_filter_callback': lambda: {'term': {'uri': 'foo'}}
        }
    },
    'items_foo': {
        'schema': {
            'uri': {'type': 'keyword'},
            'firstcreated': {'type': 'datetime', "ignore_unmapped" : True},
        },
        'datasource': {
            'backend': 'elastic',
        },
        'elastic_prefix': 'FOO',
    },
    'items_foo_default_index': {
        'schema': {
            'uri': {'type': 'keyword'}
        },
        'datasource': {
            'source': 'items_foo',
            'backend': 'elastic'
        },
    },
}


ELASTICSEARCH_SETTINGS = {
    'settings': {
        'analysis': {
            'analyzer': {
                'phrase_prefix_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'keyword',
                    'filter': ['lowercase']
                }
            }
        }
    }
}

HIGHLIGHT = {
    'pre_tags': ['<span class=\"es-highlight\">'],
    'post_tags': ['</span>'],
    'fields': {
        'name': {'number_of_fragments': 0},
        'description': {'number_of_fragments': 0},
    }
}



class TestElastic(TestCase):

    def drop_index(self, index):
        # index can not be dropped by alias anymore -> use index name
        # https://github.com/javanna/elasticsearch/commit/9e62978956beb4169a4ea88238d8fe34ce9f25c9
        try:
            self.es.indices.delete(index)
        except elasticsearch.exceptions.NotFoundError:
            pass

    def setUp(self):
        settings = {'DOMAIN': DOMAIN}
        settings['ELASTICSEARCH_URL'] = 'http://localhost:9200'
        settings['FOO_URL'] = settings['ELASTICSEARCH_URL']
        self.es = elasticsearch.Elasticsearch([settings['ELASTICSEARCH_URL']])
        self.app = eve.Eve(settings=settings, data=Elastic)

        with self.app.app_context():
            # drop existing indexes first
            for item in settings["DOMAIN"].items():
                self.drop_index(item[0])
            self.app.data.init_index(self.app)

    def test_parse_date(self):
        date = parse_date('2013-11-06T07:56:01.414944+00:00')
        self.assertIsInstance(date, datetime)
        self.assertEqual('07:56+0000', date.strftime('%H:%M%z'))

    def test_parse_date_with_null(self):
        date = parse_date(None)
        self.assertIsNone(date)

    def test_generate_index_name(self):
        self.assertNotEqual(generate_index_name('a'), generate_index_name('a'))

    def test_put_mapping(self):
        elastic = Elastic(None)
        elastic.init_app(self.app)
        with self.app.app_context():
            elastic.put_mapping(self.app)

        mapping = elastic.get_mapping()
        print(mapping)
        self.assertNotIn('published_items', mapping)

        items_mapping = mapping['items']['mappings']['doc']['properties']

        self.assertIn('firstcreated', items_mapping)
        self.assertEqual('date', items_mapping['firstcreated']['type'])

        self.assertIn(config.DATE_CREATED, items_mapping)
        self.assertIn(config.LAST_UPDATED, items_mapping)

        self.assertIn('uri', items_mapping)
        self.assertIn('category', items_mapping)

        self.assertIn('dateline', items_mapping)
        dateline_mapping = items_mapping['dateline']
        self.assertIn('created', dateline_mapping['properties'])
        self.assertEqual('date', dateline_mapping['properties']['created']['type'])

        self.assertIn('place', items_mapping)
        place_mapping = items_mapping['place']
        self.assertIn('created', place_mapping['properties'])
        self.assertEqual('date', place_mapping['properties']['created']['type'])

    def test_dates_are_parsed_on_fetch(self):
        with self.app.app_context():
            ids = self.app.data.insert('items', [{'uri': 'test', 'firstcreated': '2012-10-10T11:12:13+0000'}])
            item = self.app.data.find_one('items', req=None, uri='test')
            self.assertIsInstance(item['firstcreated'], datetime)

    def test_bulk_insert(self):
        with self.app.app_context():
            (count, _errors) = self.app.data.bulk_insert('items_with_description', [
                {'_id': 'u1', 'uri': 'u1', 'name': 'foo', 'firstcreated': '2012-01-01T11:12:13+0000'},
                {'_id': 'u2', 'uri': 'u2', 'name': 'foo', 'firstcreated': '2013-01-01T11:12:13+0000'},
                {'_id': 'u3', 'uri': 'u3', 'name': 'foo', 'description': 'test', 'firstcreated': '2013-01-01T11:12:13+0000'},
            ])
            self.assertEquals(3, count)
            self.assertEquals(0, len(_errors))

    def test_query_filter_with_filter_dsl_and_schema_filter(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [
                {'uri': 'u1', 'name': 'foo', 'firstcreated': '2012-01-01T11:12:13+0000'},
                {'uri': 'u2', 'name': 'foo', 'firstcreated': '2013-01-01T11:12:13+0000'},
                {'uri': 'u3', 'name': 'foo', 'description': 'test', 'firstcreated': '2013-01-01T11:12:13+0000'},
            ])

        query_filter = {
            'term': {'name': 'foo'}
        }

        with self.app.app_context():
            req = ParsedRequest()
            req.args = {'filter': json.dumps(query_filter)}
            self.assertEqual(1, self.app.data.find('items_with_description', req, None).count())

        with self.app.app_context():
            req = ParsedRequest()
            req.args = {'q': 'bar', 'filter': json.dumps(query_filter)}
            self.assertEqual(0, self.app.data.find('items_with_description', req, None).count())

    def test_find_one_by_id(self):
        """elastic 1.0+ is using 'found' property instead of 'exists'"""
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'test', config.ID_FIELD: 'testid'}])
            item = self.app.data.find_one('items', req=None, **{config.ID_FIELD: 'testid'})
            self.assertEqual('testid', item[config.ID_FIELD])

    def test_find_one_multiple_criteria(self):
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'test', 'name': 'foo', config.ID_FIELD: 'testid'}])
            item = self.app.data.find_one('items', req=None, name='foo', uri='test')
            self.assertEqual('testid', item[config.ID_FIELD])

    def test_formating_fields(self):
        """when using elastic 1.0+ it puts all requested fields values into a list
        so instead of {"name": "test"} it returns {"name": ["test"]}"""
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'test', 'name': 'test'}])
            item = self.app.data.find_one('items', req=None, uri='test')
            self.assertEqual('test', item['name'])

    def test_search_via_source_param(self):
        query = {'query': {'term': {'uri': 'foo'}}}
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}])
            self.app.data.insert('items', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {'source': json.dumps(query)}
            res = self.app.data.find('items', req, None)
            self.assertEqual(1, res.count())

    def test_search_via_source_param_and_schema_filter(self):
        query = {'query': {'term': {'uri': 'foo'}}}
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo', 'description': 'test', 'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {'source': json.dumps(query)}
            res = self.app.data.find('items_with_description', req, None)
            self.assertEqual(1, res.count())

    def test_search_via_source_param_and_with_highlight(self):
        query = {'query': {'match': {'all': 'foo'}}}
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo',
                                                             'description': 'This is foo',
                                                             'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {'source': json.dumps(query), 'es_highlight': 1}
            res = self.app.data.find('items_with_description', req, None)
            self.assertEqual(1, res.count())
            es_highlight = res[0].get('es_highlight')
            self.assertIsNotNone(es_highlight)
            self.assertEqual(es_highlight.get('name')[0], '<span class=\"es-highlight\">foo</span>')
            self.assertEqual(es_highlight.get('description')[0], 'This is <span class=\"es-highlight\">foo</span>')

    def test_search_via_source_param_and_without_highlight(self):
        query = {'query': {'query_string': {'query': 'foo'}}}
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo',
                                                             'description': 'This is foo',
                                                             'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {'source': json.dumps(query), 'es_highlight': 0}
            res = self.app.data.find('items_with_description', req, None)
            self.assertEqual(1, res.count())
            es_highlight = res[0].get('es_highlight')
            self.assertIsNone(es_highlight)

    def test_search_via_source_param_and_with_source_projection(self):
        query = {'query': {'query_string': {'query': 'foo'}}}
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo',
                                                             'description': 'This is foo',
                                                             'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'bar', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {'source': json.dumps(query), 'projections': json.dumps(["name"])}
            res = self.app.data.find('items_with_description', req, None)
            self.assertEqual(1, res.count())
            self.assertTrue('description' not in res.docs[0])
            self.assertTrue('name' in res.docs[0])

    def test_should_aggregate(self):
        with self.app.app_context():
            self.app.config['ELASTICSEARCH_AUTO_AGGREGATIONS'] = False
            req = ParsedRequest()
            req.args = {'aggregations': 1}
            self.assertTrue(self.app.data.should_aggregate(req))
            req.args = {'aggregations': '0'}
            self.assertFalse(self.app.data.should_aggregate(req))

    def test_should_project(self):
        with self.app.app_context():
            req = ParsedRequest()
            req.args = {'projections': json.dumps(["priority", "urgency", "word_count", "slugline", "highlights"])}
            self.assertTrue(self.app.data.should_project(req))
            req.args = {'projections': json.dumps([])}
            self.assertFalse(self.app.data.should_project(req))
            req.args = {}
            self.assertFalse(self.app.data.should_project(req))

    def test_get_projected_fields(self):
        with self.app.app_context():
            req = ParsedRequest()
            req.args = {'projections': json.dumps(["priority", "urgency", "word_count", "slugline", "highlights"])}
            fields = self.app.data.get_projected_fields(req)
            self.assertEqual(fields, "priority,urgency,word_count,slugline,highlights")

    def test_should_highlight(self):
        with self.app.app_context():
            req = ParsedRequest()
            req.args = {'es_highlight': 1}
            self.assertTrue(self.app.data.should_highlight(req))
            req.args = {'es_highlight': '0'}
            self.assertFalse(self.app.data.should_highlight(req))

    def test_mapping_is_there_after_delete(self):
        with self.app.app_context():
            self.app.data.put_mapping(self.app)
            mapping = self.app.data.get_mapping('items', 'doc')
            self.app.data.remove('items')
            self.assertEqual(mapping, self.app.data.get_mapping('items', 'doc'))

    def test_find_one_raw(self):
        with self.app.app_context():
            ids = self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}])
            item = self.app.data.find_one_raw('items', ids[0])
            self.assertEqual(item['name'], 'foo')

    def test_is_empty(self):
        with self.app.app_context():
            self.assertTrue(self.app.data.is_empty('items'))
            self.app.data.insert('items', [{'uri': 'foo'}])
            self.assertFalse(self.app.data.is_empty('items'))

    def test_replace(self):
        with self.app.app_context():
            res = self.app.data.insert('items', [{'uri': 'foo'}])
            self.assertEqual(1, len(res))
            new_item = {'uri': 'bar'}
            res = self.app.data.replace('items', res.pop(), new_item)
            self.assertEqual(2, res['_version'])

    def test_sub_resource_lookup(self):
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}])
            req = ParsedRequest()
            req.args = {}
            self.assertEqual(1, self.app.data.find('items', req, {'name': 'foo'}).count())
            self.assertEqual(0, self.app.data.find('items', req, {'name': 'bar'}).count())
            self.assertEqual(1, self.app.data.find('items', req, {'name': 'foo', 'uri': 'foo'}).count())

    def test_sub_resource_lookup_with_schema_filter(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo', 'description': 'test', 'name': 'foo'}])
            req = ParsedRequest()
            req.args = {}
            self.assertEqual(1, self.app.data.find('items_with_description', req, {'name': 'foo'}).count())
            self.assertEqual(0, self.app.data.find('items_with_description', req, {'name': 'bar'}).count())

    def test_resource_filter(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo', 'description': 'test'}, {'uri': 'bar'}])
            req = ParsedRequest()
            req.args = {}
            req.args['source'] = json.dumps({'query': {'filter': {'term': {'uri': 'bar'}}}})
            self.assertEqual(0, self.app.data.find('items_with_description', req, None).count())

    def test_where_filter(self):
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'foo', 'name': 'foo'}, {'uri': 'bar', 'name': 'bar'}])

        with self.app.test_client() as c:
            response = c.get('items?where={"name":"foo"}')
            data = json.loads(response.data)
            self.assertEqual(1, len(data['_items']))

            response = c.get('items?where=name=="foo"')
            data = json.loads(response.data)
            self.assertEqual(1, len(data['_items']))

    def test_update(self):
        with self.app.app_context():
            ids = self.app.data.insert('items', [{'uri': 'foo'}])
            self.app.data.update('items', ids[0], {'uri': 'bar', '_id': ids[0], '_type': 'items'})
            self.assertEqual(self.app.data.find_one('items', req=None, _id=ids[0])['uri'], 'bar')

    def test_remove_by_id(self):
        with self.app.app_context():
            self.ids = self.app.data.insert('items', [{'uri': 'foo'}, {'uri': 'bar'}])
            self.app.data.remove('items', {'_id': self.ids[0]})
            req = ParsedRequest()
            req.args = {}
            self.assertEqual(1, self.app.data.find('items', req, None).count())

    def test_remove_non_existing_item(self):
        with self.app.app_context():
            self.assertEqual(self.app.data.remove('items', {'_id': 'notfound'}), None)

    @raises(elasticsearch.exceptions.ConnectionError)
    def test_it_can_use_configured_url(self):
        with self.app.app_context():
            self.app.config['ELASTICSEARCH_URL'] = 'http://localhost:9292'
            elastic = Elastic(self.app)
            elastic.init_index(self.app)

    def test_resource_aggregates(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo1', 'description': 'test', 'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'foo2', 'description': 'test1', 'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'foo3', 'description': 'test2', 'name': 'foo'}])
            self.app.data.insert('items_with_description', [{'uri': 'bar1', 'description': 'test3', 'name': 'bar'}])
            req = ParsedRequest()
            req.args = {}
            response = {}
            item1 = self.app.data.find('items_with_description', req, {'name': 'foo'})
            item2 = self.app.data.find('items_with_description', req, {'name': 'bar'})
            item1.extra(response)
            self.assertEqual(3, item1.count())
            self.assertEqual(1, item2.count())
            self.assertEqual(3, response['_aggregations']['type']['buckets'][0]['doc_count'])

    def test_resource_aggregates_no_auto(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [{'uri': 'foo'}])
            self.app.config['ELASTICSEARCH_AUTO_AGGREGATIONS'] = False
            req = ParsedRequest()
            req.args = {}
            response = {}
            cursor = self.app.data.find('items_with_description', req, {})
            cursor.extra(response)
            self.assertNotIn('_aggregations', response)

            req.args = {'aggregations': 1}
            cursor = self.app.data.find('items_with_description', req, {})
            cursor.extra(response)
            self.assertIn('_aggregations', response)

    def test_put(self):
        with self.app.app_context():
            self.app.data.replace('items', 'newid', {'uri': 'foo', '_id': 'newid', '_type': 'x'})
            self.assertEqual('foo', self.app.data.find_one('items', None, _id='newid')['uri'])

    def test_args_filter(self):
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'foo'}, {'uri': 'bar'}])
            req = ParsedRequest()
            req.args = {}
            req.args['filter'] = json.dumps({'term': {'uri': 'foo'}})
            self.assertEqual(1, self.app.data.find('items', req, None).count())

    def test_filters_with_aggregations(self):
        with self.app.app_context():
            self.app.data.insert('items_with_description', [
                {'uri': 'foo', 'name': 'test', 'description': 'a'},
                {'uri': 'bar', 'name': 'test', 'description': 'b'},
            ])

            req = ParsedRequest()
            res = {}
            cursor = self.app.data.find('items_with_description', req, {'uri': 'bar'})
            cursor.extra(res)
            self.assertEqual(1, cursor.count())
            self.assertIn({'key': 'test', 'doc_count': 1}, res['_aggregations']['type']['buckets'])
            self.assertEqual(1, res['_aggregations']['type']['buckets'][0]['doc_count'],)

    def test_filter_without_args(self):
        with self.app.app_context():
            self.app.data.insert('items', [{'uri': 'foo'}, {'uri': 'bar'}])
            req = ParsedRequest()
            self.assertEqual(2, self.app.data.find('items', req, None).count())
            self.assertEqual(1, self.app.data.find('items', req, {'uri': 'foo'}).count())

    def test_filters_with_filtered_query(self):
        with self.app.app_context():
            self.app.data.insert('items', [
                {'uri': 'foo'},
                {'uri': 'bar'},
                {'uri': 'baz'},
            ])

            query = {'query': {'filter': [
                {'term': {'uri': 'foo'}},
                {'term': {'uri': 'bar'}},
            ]}}

            req = ParsedRequest()
            req.args = {'source': json.dumps(query)}
            cursor = self.app.data.find('items', req, None)
            self.assertEqual(0, cursor.count())

    def test_basic_search_query(self):
        with self.app.app_context():
            self.app.data.insert('items', [
                {'uri': 'foo'},
                {'uri': 'bar'}
            ])

        with self.app.test_request_context('/items/?q=foo'):
            req = parse_request('items')
            cursor = self.app.data.find('items', req, None)
            self.assertEquals(1, cursor.count())

    def test_phrase_search_query(self):
        with self.app.app_context():
            self.app.data.insert('items', [
                {'uri': 'foo bar'},
                {'uri': 'some text'}
            ])

        with self.app.test_request_context('/items/?q="foo bar"'):
            req = parse_request('items')
            cursor = self.app.data.find('items', req, None)
            self.assertEquals(1, cursor.count())

        with self.app.test_request_context('/items/?q="bar foo"'):
            req = parse_request('items')
            cursor = self.app.data.find('items', req, None)
            self.assertEquals(0, cursor.count())

    def test_elastic_filter_callback(self):
        with self.app.app_context():
            self.app.data.insert('items_with_callback_filter', [
                {'uri': 'foo'},
                {'uri': 'bar'},
            ])

        with self.app.test_request_context():
            req = parse_request('items_with_callback_filter')
            cursor = self.app.data.find('items_with_callback_filter', req, None)
            self.assertEqual(1, cursor.count())

    def test_elastic_sort_by_score_if_there_is_query(self):
        with self.app.app_context():
            self.app.data.insert('items', [
                {'uri': 'foo', 'name': 'foo bar'},
                {'uri': 'bar', 'name': 'foo bar'}
            ])

        with self.app.test_request_context('/items/'):
            req = parse_request('items')
            req.args = {'q': 'foo'}
            cursor = self.app.data.find('items', req, None)
            self.assertEqual(2, cursor.count())
            self.assertEqual('foo', cursor[0]['uri'])

    def test_elastic_find_default_sort_no_mapping(self):
        with self.app.test_request_context('/items/'):
            req = parse_request('items')
            req.args = {}
            cursor = self.app.data.find('items', req, None)
            self.assertEqual(0, cursor.count())

    def test_no_force_refresh(self):
        with self.app.app_context():
            self.app.config['ELASTICSEARCH_FORCE_REFRESH'] = False
            ids = self.app.data.insert('items', [
                {'uri': 'foo', 'name': 'foo'},
                {'uri': 'bar', 'name': 'bar'},
            ])

            item = self.app.data.find_one('items', req=None, _id=ids[0])
            self.assertEqual('foo', item['uri'])

            time.sleep(2)
            req = ParsedRequest()
            cursor = self.app.data.find('items', req, None)
            self.assertEqual(2, cursor.count())

    def test_elastic_prefix(self):
        self.drop_index('foo')
        with self.app.app_context():
            self.app.data.init_index()

            self.app.data.insert('items_foo_default_index', [{'uri': 'test'}])
            foo_items = self.app.data.find('items_foo', ParsedRequest(), None)
            self.assertEqual(0, foo_items.count())

            self.app.data.insert('items_foo', [{'uri': 'foo'}, {'uri': 'bar'}])
            foo_items = self.app.data.find('items_foo', ParsedRequest(), None)
            self.assertEqual(2, foo_items.count())

    def test_retry_on_conflict(self):
        with self.app.app_context():
            original_method = self.app.data.elastic('items').update
            update_mock = MagicMock()
            self.app.data.elastic('items').update = update_mock

            self.app.data.update('items', 'foo', {'uri': 'bar', '_id': 'foo', '_type': 'items'})
            self.assertEqual(update_mock.call_count, 1)
            self.assertIn('retry_on_conflict', update_mock.call_args[1])
            self.assertEqual(update_mock.call_args[1]['retry_on_conflict'], 5)

            self.app.config['ELASTICSEARCH_RETRY_ON_CONFLICT'] = 1
            self.app.data.update('items', 'foo', {'uri': 'bar', '_id': 'foo', '_type': 'items'})
            self.assertEqual(update_mock.call_count, 2)
            self.assertIn('retry_on_conflict', update_mock.call_args[1])
            self.assertEqual(update_mock.call_args[1]['retry_on_conflict'], 1)

            self.app.config['ELASTICSEARCH_RETRY_ON_CONFLICT'] = None
            self.app.data.update('items', 'foo', {'uri': 'bar', '_id': 'foo', '_type': 'items'})
            self.assertEqual(update_mock.call_count, 3)
            self.assertNotIn('retry_on_conflict', update_mock.call_args[1])
            self.app.data.elastic('items').update = original_method


class TestElasticSearchParentChild(TestCase):
    index_name = 'elastic_settings'

    def setUp(self):
        settings = {
            'DOMAIN': {
                'items': {
                    'schema': {
                        'slugline': {
                            'type': 'text',
                            'mapping': {
                                'type': 'text',
                                'fields': {
                                    'phrase': {
                                        'type': 'text',
                                        'analyzer': 'phrase_prefix_analyzer',
                                        'search_analyzer': 'phrase_prefix_analyzer'
                                    }
                                }
                            }
                        }
                    },
                    'datasource': {
                        'backend': 'elastic'
                    }
                }
            },
            'ELASTICSEARCH_URL': 'http://localhost:9200',
            'ELASTICSEARCH_SETTINGS': ELASTICSEARCH_SETTINGS
        }

        self.app = eve.Eve(settings=settings, data=Elastic)
        with self.app.app_context():
            self.app.data.init_index(self.app)
            for resource in self.app.config['DOMAIN']:
                self.app.data.remove(resource)

            self.es = get_es(self.app.config.get('ELASTICSEARCH_URL'))

    def tearDown(self):
        """ Cleanup index after finished tests."""
        with self.app.app_context():
            get_indices(self.es).delete(self.index_name)

    def test_elastic_settings(self):
        with self.app.app_context():
            settings = self.app.data.get_settings(self.index_name)
            analyzer = settings['settings']['index']['analysis']['analyzer']
            self.assertDictEqual({
                'phrase_prefix_analyzer': {
                    'tokenizer': 'keyword',
                    'filter': ['lowercase'],
                    'type': 'custom'
                }
            }, analyzer)

    def test_put_settings(self):
        with self.app.app_context():
            settings = self.app.data.get_settings(self.index_name)
            analyzer = settings['settings']['index']['analysis']['analyzer']
            self.assertDictEqual({
                'phrase_prefix_analyzer': {
                    'tokenizer': 'keyword',
                    'filter': ['lowercase'],
                    'type': 'custom'
                }
            }, analyzer)

            new_settings = deepcopy(ELASTICSEARCH_SETTINGS)

            new_settings['settings']['analysis']['analyzer']['phrase_prefix_analyzer'] = {
                'type': 'custom',
                'tokenizer': 'whitespace',
                'filter': ['uppercase']
            }

            self.app.data.put_settings(self.app, self.index_name, new_settings)
            settings = self.app.data.get_settings(self.index_name)
            analyzer = settings['settings']['index']['analysis']['analyzer']
            self.assertDictEqual({
                'phrase_prefix_analyzer': {
                    'tokenizer': 'whitespace',
                    'filter': ['uppercase'],
                    'type': 'custom'
                }
            }, analyzer)

    def test_put_settings_existing_index(self):
        with self.app.app_context():
            self.app.config['DOMAIN']['items']['schema']['slugline'] = {
                'type': 'text',
                'mapping': {
                    'type': 'text',
                    'fields': {
                        'phrases': {
                            'type': 'text',
                            'analyzer': 'prefix_analyzer',
                            'search_analyzer': 'prefix_analyzer'
                        }
                    }
                }
            }

            new_settings = deepcopy(ELASTICSEARCH_SETTINGS)
            new_settings['settings']['analysis']['analyzer']['prefix_analyzer'] = {
                'type': 'custom',
                'tokenizer': 'whitespace',
                'filter': ['uppercase']
            }

            self.app.config['ELASTICSEARCH_SETTINGS'] = new_settings

            if hasattr(self, 'assertLogs'):
                with self.assertLogs('elastic') as log:
                    self.app.data.put_mapping(self.app)
                    self.assertIn('ERROR:elastic:mapping error, updating settings resource=items', log.output[0])
            else:
                self.app.data.put_mapping(self.app)

    def test_cluster(self):
        es = get_es(['http://localhost:9200', 'http://localhost:9200'])
        self.assertIsNotNone(es)

    def test_serializer_config(self):
        class TestSerializer(elasticsearch.JSONSerializer):
            pass

        es = get_es('http://localhost:9200', serializer=TestSerializer())
        self.assertIsInstance(es.transport.serializer, TestSerializer)


class TestElasticSearchParentChild(TestCase):
    """ ES 6.0 uses parent-join field to handle parent child relationships,
    The join field shouldn’t be used like joins in a relation database.
    In Elasticsearch the key to good performance is to de-normalize your data into documents.
    Each join field, has_child or has_parent query adds a significant tax to the query performance.
    Here we add a join_field to the datasource with key 'join_field' and its name
    https://www.elastic.co/guide/en/elasticsearch/reference/6.0/parent-join.html
    """
    index_name = 'library'
    version_2x = False

    domain = {
        'library': {
            'schema': {
                'name': {'type': 'text'},
                'introduction': {'type': 'text'},
                'join_field':  {
                    'type':'join',
                    'relations': { 'library': 'book'}
                    }
            },
            'datasource': {
                'backend': 'elastic',
                'join_field':'join_field'
            }
        }
    }

    def setUp(self):
        settings = {
            'DOMAIN': self.domain,
            'ELASTICSEARCH_URL': 'http://localhost:9200',
            'ELASTICSEARCH_SETTINGS': ELASTICSEARCH_SETTINGS
        }

        self.app = eve.Eve(settings=settings, data=Elastic)
        with self.app.app_context():
            self.app.data.init_index(self.app)
            for resource in self.app.config['DOMAIN']:
                self.app.data.remove(resource)

            self.es = get_es(self.app.config.get('ELASTICSEARCH_URL'))
            self.checkVersion()

    def tearDown(self):
        """ Cleanup all indeces after finished tests."""
        with self.app.app_context():
            for item in self.domain.items():
                get_indices(self.es).delete(item[0], ignore=[400, 404])

    def checkVersion(self):
        with self.app.app_context():
            info = self.es.info()
            self.version_2x = info.get('version', {}).get('number', '').startswith('2')

    def test_child_items_mapping(self):
        """ Chceck if mapping for child items is done properly and adds a relations field.
        refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/parent-join.html
        """
        with self.app.app_context():
            mapping = self.es.indices.get_mapping(index='library', doc_type='doc')

            self.assertIn('library', mapping)
            print(mapping)
            self.assertDictEqual(mapping.get('library').get('mappings').get('doc').get('properties').get('join_field').get('relations'), {'library':'book'})
            self.assertEqual(mapping.get('library').get('mappings').get('doc').get('properties').get('join_field').get('eager_global_ordinals'), True)

    def test_insert_child_item(self):
        with self.app.app_context():
            self.app.data.insert(self.index_name, [{'_id': 'foo', 'name': 'library', 'introduction':'I am a the library', 'join_field': 'library'}])
            self.app.data.insert(self.index_name, [{'_id': 'childfoo', 'name': 'book', 'introduction': 'I am a book1', 'join_field': {'name':'book', 'parent':'foo'}}
            ])

            parent = self.app.data.find_one(self.index_name, req=None, _id='foo')
            self.assertEqual(parent['_id'], 'foo')
            self.assertEqual(parent['name'], 'library')

            child = self.app.data.find_one(self.index_name, req=None, _id='childfoo', parent='foo')
            self.assertEqual(child['_id'], 'childfoo')
            self.assertEqual(child['name'], 'book')
            self.assertEqual(child['join_field']['parent'], 'foo')

            # without parent
            child = self.app.data.find_one(self.index_name, req=None, _id='childfoo')
            self.assertEqual(child['_id'], 'childfoo')
            self.assertEqual(child['name'], 'book')
            self.assertEqual(child['join_field']['parent'], 'foo')

    def test_insert_child_item_with_no_parent(self):
        with self.app.app_context():
            self.app.data.insert(self.index_name, [
                {'_id': 'childfoo', 'name': 'childfoo', 'introduction': 'I am a book', 'join_field': {'name':'book', 'parent':'foo'} }
            ])

            child = self.app.data.find_one(self.index_name, req=None, _id='childfoo', parent='foo')
            self.assertEqual(child['_id'], 'childfoo')
            self.assertEqual(child['name'], 'childfoo')
            self.assertEqual(child['join_field']['parent'], 'foo')

    def test_update_child_item(self):
        with self.app.app_context():
            self.app.data.insert(self.index_name, [{'_id': 'foo_l_update', 'name': 'library', 'introduction':'I am a the library', 'join_field': 'library'}])
            self.app.data.insert(self.index_name, [
                {'_id': 'foo_b_update', 'name': 'book', 'introduction':'I am a book of the library', 'join_field': {'name':'book', 'parent':'foo_l_update'}}
            ])

            child = self.app.data.find_one(self.index_name, req=None, _id='foo_b_update', parent='foo_l_update')

            self.assertEqual(child['_id'], 'foo_b_update')
            self.assertEqual(child['name'], 'book')
            self.assertEqual(child['introduction'], 'I am a book of the library')

            self.app.data.update(self.index_name,
                                 id_='foo_b_update',
                                 updates={'_id': 'foo_b_update', 'name': 'test',
                                           'introduction': 'test test'})

            child = self.app.data.find_one(self.index_name, req=None, _id='foo_b_update', parent='foo_l_update')
            self.assertEqual(child['_id'], 'foo_b_update')
            self.assertEqual(child['name'], 'test')
            self.assertEqual(child['introduction'], 'test test')

    def test_bulk_insert_child_items(self):
        """ basically the first line in a bulk insert is a representation of the request parameters that you would use in a single request
        """
        with self.app.app_context():
            (count, _errors) = self.app.data.bulk_insert(self.index_name, [
                {'_id': 'b1', 'name': 'foo', 'introduction': 'I am a the library', 'join_field': 'library', '_routing': 'b1'},
                {'_id': 'u1', 'name': 'foo', 'introduction': 'I am a book1', 'join_field': {'name':'book', 'parent':'b1'}, '_routing': 'b1'},
                {'_id': 'u2', 'name': 'foo', 'introduction': 'I am a book2', 'join_field': {'name':'book', 'parent':'b1'}, '_routing': 'b1'},
                {'_id': 'u3', 'name': 'foo', 'introduction': 'I am a book3', 'join_field': {'name':'book', 'parent':'b1'}, '_routing': 'b1'},
            ])
            self.assertEquals(4, count)
            self.assertEquals(0, len(_errors))

    def test_replace_child_item(self):
        """ checks if a child can be replaced and a new parent can be set,
        replacing child items with no parents does not create execptions in parent_join relationships."""
        with self.app.app_context():
            res = self.app.data.insert(self.index_name, [{'_id': 'replace_me', 'name': 'foo', 'introduction': 'I am a book1', 'join_field': {'name':'book', 'parent':'replace_me_l'}}])
            self.assertEqual(1, len(res))
            new_item ={'_id': 'replace_me', 'name': 'bar', 'introduction': 'I am a book1', 'join_field': {'name':'book', 'parent':'b1'}}
            res = self.app.data.replace(self.index_name, 'replace_me', new_item)
            self.assertEqual(2, res['_version'])

    def test_parent_child_query(self):
        with self.app.app_context():
            self.app.data.insert(self.index_name, [
                {'_id': 'foo', 'name': 'foo', 'headline': 'test', 'introduction': 'I am a library1', 'join_field': 'library'},
                {'_id': 'bar', 'name': 'bar', 'headline': 'test', 'introduction': 'I am a library2', 'join_field': 'library'}
            ])
            self.app.data.insert(self.index_name, [
                {'_id': 'child1', 'name': 'child1', 'item_id': 'foo', 'introduction': 'I am a book of library1', 'join_field': {'name':'book', 'parent':'foo'}},
                {'_id': 'child2', 'name': 'child2', 'item_id': 'bar', 'introduction': 'I am a book of library2', 'join_field': {'name':'book', 'parent':'bar'}}
            ])

            query = {
                'query': {
                    'bool': {
                        'must': {
                            'has_child': {
                                'type': 'book',
                                'query': {
                                    'match': {'name': 'child1'}
                                }
                            }
                        }
                    }
                }
            }
            req = ParsedRequest()
            req.args = {'source': json.dumps(query)}
            results = self.app.data.find(self.index_name, req, None)
            self.assertEqual(1, results.count())
            self.assertEqual(results[0].get('_id'), 'foo')
            self.assertEqual(results[0].get('introduction'), 'I am a library1')

    def test_remove_child(self):
        with self.app.app_context():
            self.app.data.insert(self.index_name, [{'_id': 'foo', 'name': 'foo', 'headline': 'test', 'introduction': 'I am a library1', 'join_field': 'library'}])
            self.app.data.insert(self.index_name, [
                {'_id': 'childfoo', 'name': 'child1', 'introduction': 'I am a book of library1', 'join_field': {'name':'book', 'parent':'foo'}}
            ])
            self.app.data.remove(self.index_name, {'_id': 'childfoo'}, 'foo')
            child = self.app.data.find_one(self.index_name, req=None, _id='childfoo', parent='foo')
            self.assertIsNone(child)
