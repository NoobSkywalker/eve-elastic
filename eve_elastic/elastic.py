
import ast
import json
import arrow
import ciso8601
import pytz  # NOQA
import logging
import elasticsearch

from bson import ObjectId
from elasticsearch.helpers import bulk, reindex as reindex_new
from .helpers import reindex as reindex_old

from uuid import uuid4
from flask import request, abort
from eve.utils import config
from eve.io.base import DataLayer
from eve.io.mongo.parser import parse, ParseError
from arrow.parser import ParserError

logging.basicConfig()
logger = logging.getLogger('elastic')


def parse_date(date_str):
    """Parse elastic datetime string."""
    if not date_str:
        return None

    try:
        date = ciso8601.parse_datetime(date_str)
        if not date:
            date = arrow.get(date_str).datetime
    except TypeError:
        date = arrow.get(date_str[0]).datetime
    except ParserError:
        return None
    return date


def get_dates(schema):
    """Return list of datetime fields for given schema."""
    dates = [config.LAST_UPDATED, config.DATE_CREATED]
    for field, field_schema in schema.items():
        if 'type' in field_schema:
            if field_schema['type'] == 'datetime':
                dates.append(field)
    return dates


def format_doc(hit, schema, dates):
    """Format given doc to match given schema."""
    doc = hit.get('_source', {})
    doc.setdefault(config.ID_FIELD, hit.get('_id'))
    doc.setdefault('_type', hit.get('_type'))
    if hit.get('highlight'):
        doc['es_highlight'] = hit.get('highlight')

    for key in dates:
        if key in doc:
            doc[key] = parse_date(doc[key])

    return doc


def noop():
    """no-op."""
    pass


def is_elastic(datasource):
    """Detect if given resource uses elastic."""
    return datasource.get('backend') == 'elastic' or datasource.get('search_backend') == 'elastic'


def generate_index_name(alias):
    """Generate a random name alias for an index."""
    random = str(uuid4()).split('-')[0]
    return '{}_{}'.format(alias, random)


def reindex(es, source, dest):
    """Call reindex with appropriate version."""
    version = es.info().get('version').get('number')
    if version.startswith('1.'):
        return reindex_old(es, source, dest)
    else:
        return reindex_new(es, source, dest)


class InvalidSearchString(Exception):
    """Exception thrown when search string has invalid value."""
    pass


class InvalidIndexSettings(Exception):
    """Exception is thrown when put_settings is called without ELASTIC_SETTINGS."""
    pass


class ElasticJSONSerializer(elasticsearch.JSONSerializer):
    """Customize the JSON serializer used in Elastic."""
    def default(self, value):
        """Convert mongo.ObjectId."""
        if isinstance(value, ObjectId):
            return str(value)
        return super(ElasticJSONSerializer, self).default(value)


class ElasticCursor(object):
    """Search results cursor."""

    no_hits = {'hits': {'total': 0, 'hits': []}}

    def __init__(self, hits=None, docs=None):
        """Parse hits into docs."""
        self.hits = hits if hits else self.no_hits
        self.docs = docs if docs else []

    def __getitem__(self, key):
        """Return a specific document item."""
        return self.docs[key]

    def first(self):
        """Get first doc."""
        return self.docs[0] if self.docs else None

    def count(self, **kwargs):
        """Get hits count."""
        return int(self.hits.get('hits', {}).get('total', '0'))

    def extra(self, response):
        """Add extra info to response."""
        if 'facets' in self.hits:
            response['_facets'] = self.hits['facets']
        if 'aggregations' in self.hits:
            response['_aggregations'] = self.hits['aggregations']


def set_filters(query, must_filter, base_filters=None):
    """Put together must_filters and base_filters that are requested and set them as filter or accordingly.

    :param query: elastic query being constructed
    :param base_filters: all filters defined in the mapping already (elastic_filter_callback, elastic_filter)
    :param must_filters: all filters set inside the query (eg. resource config, sub_resource_lookup)
    """

    filters = [f for f in base_filters if f is not None]
    must_filters = [f for f in must_filter if f is not None]

    query_filter = query['query'].get('filter', None)

    if query_filter is not None:
        filters = query_filter

    if 'bool' not in query['query'] and (must_filters or filters):
        query['query']['bool'] = {}

    if must_filters:
        query['query']['bool']['must'] = must_filters
    if filters:
        query['query']['bool']['filter'] = filters


def set_sort(query, sort):
    """Set the sorting on the sorted value to the field.

    For reference see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html
    """
    query['sort'] = []
    for (key, sortdir) in sort:
        # check sort order
        sort_order = dict([("order", 'asc' if sortdir > 0 else 'desc')])
        sort_dict = dict([(key, sort_order)])
        ignore_unmapped = {"unmapped_type": "long"}

        # append sort order and unmapped_type to the key.
        sort_dict[key].update(ignore_unmapped)
        query['sort'].append(sort_dict)


def get_es(url, **kwargs):
    """Create elasticsearch client instance.

    :param url: elasticsearch url
    """
    urls = [url] if isinstance(url, str) else url
    kwargs.setdefault('serializer', ElasticJSONSerializer())
    es = elasticsearch.Elasticsearch(urls, **kwargs)
    return es


def get_indices(es):
    """Return elasticsearch indeces."""
    return es.indices


class Elastic(DataLayer):
    """ElasticSearch data layer."""

    serializers = {
        'integer': int,
        'datetime': parse_date,
        'objectid': ObjectId,
    }

    def __init__(self, app=None, **kwargs):
        """Let user specify extra arguments for Elasticsearch."""
        self.app = app
        self.kwargs = kwargs
        self.elastics = {}
        super(Elastic, self).__init__(app)

    def init_app(self, app):
        """Initialize the application."""
        app.config.setdefault('ELASTICSEARCH_URL', 'http://localhost:9200/')

        # domain items now need to create an index for each document type
        # therefore on init we set create the index relations by providing a value in resource
        app.config.setdefault('ELASTICSEARCH_INDEXES', {})

        # possibility to add an Index prefix which then creates and queries the index with a prefix.
        # This helps to prevent issues on index names with other applications using the same elastic cluster
        app.config.setdefault('ELASTICSEARCH_INDEX_PREFIX', '')

        app.config.setdefault('ELASTICSEARCH_FORCE_REFRESH', True)
        app.config.setdefault('ELASTICSEARCH_AUTO_AGGREGATIONS', True)

        self.app = app
        self.es = get_es(app.config['ELASTICSEARCH_URL'], **self.kwargs)

    def init_index(self, app=None):
        """Create indexes and put mapping."""
        elasticindexes = self._get_indexes()

        for index, settings in elasticindexes.items():
            es = settings['resource']
            if not es.indices.exists(index):
                self.create_index(index, dict([('mappings', settings.get('mappings'))]), settings.get('settings'), es)
                continue
            else:
                self.put_settings(app, index, dict([('mappings', settings.get('mappings'))]).get('settings'), es)
        return

    def _get_indexes(self):
        """Based on the resource definition calculates the index definition."""
        indexes = {}
        for resource in self._get_elastic_resources():
            try:
                index = self._resource_index(resource)

            except KeyError:  # ignore missing
                continue

            if index not in indexes:
                indexes.update({
                    index: {
                        'resource': self.elastic(resource),
                        'mappings': {
                            "doc": {
                            }
                        },
                        'settings': {
                        }
                    }
                })

            resource_config = self.app.config['DOMAIN'][resource]
            if 'settings' in resource_config:
                indexes[index]['settings'].update(resource_config['settings'])
            properties = self._get_mapping_properties(resource_config)
            indexes[index]['mappings']['doc'] = properties

        return indexes

    def get_datasource(self, resource):
        """Get datasource for given resource."""
        if hasattr(self, '_datasource'):
            return self._datasource(resource)
        return self.datasource(resource)

    def _get_mapping(self, schema):
        """Get mapping for given resource or item schema.

        :param schema: resource or dict/list type item schema
        """
        properties = {}
        for field, field_schema in schema.items():
            field_mapping = self._get_field_mapping(field_schema)
            if field_mapping:
                properties[field] = field_mapping
        return {'properties': properties}

    def _get_field_mapping(self, schema):
        """Get mapping for single field schema.

        :param schema: field schema
        """

        if 'mapping' in schema:
            return schema['mapping']
        elif schema['type'] == 'dict' and 'schema' in schema:
            return self._get_mapping(schema['schema'])
        elif schema['type'] == 'list' and 'schema' in schema.get('schema', {}):
            return self._get_mapping(schema['schema']['schema'])

        # add mapping for parent_child relations
        if schema['type'] == 'join':
            return {'type': 'join', 'relations': schema['relations']}

        # sorting for date when value is empty results in an exception
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-malformed.html
        elif schema['type'] == 'datetime' and 'ignore_malformed' in schema:
            return {'type': 'date', 'ignore_malformed': schema['ignore_malformed']}
        elif schema['type'] == 'datetime':
            return {'type': 'date'}

        # copy text and keywords field to an all field so they can still be used for query_string
        # https://github.com/elastic/elasticsearch/issues/19784
        elif schema['type'] == 'text':
            return {'type': 'text', 'copy_to': 'all'}
        elif schema['type'] == 'keyword':
            return {'type': 'keyword', 'copy_to': 'all'}

    def create_index(self, index=None, mappings=None, settings=None, es=None):
        """Create new index and ignore if it exists already."""
        if index is None:
            index = self.index
        if es is None:
            es = self.es
        try:
            # add an index_name alias
            alias = generate_index_name(index)

            args = {'index': index}
            if mappings:
                args['body'] = mappings

            if settings:
                args['body']['settings'] = settings

            es.indices.create(**args)
            es.indices.put_alias(index, alias)
            logger.info('created index alias=%s index=%s' % (alias, index))
        except elasticsearch.TransportError as e:
            logger.exception(e)

    def _get_elastic_resources(self):
        elastic_resources = {}
        for resource, resource_config in self.app.config['DOMAIN'].items():
            datasource = resource_config.get('datasource', {})

            if not is_elastic(datasource):
                continue

            if datasource.get('source', resource) != resource:  # only put mapping for core types
                continue

            elastic_resources[resource] = resource_config
        return elastic_resources

    def _put_resource_mapping(self, resource, es, force_index=None, properties=None, **kwargs):
        if not properties:
            resource_config = self.app.config['DOMAIN'][resource]
            properties = self._get_mapping_properties(resource_config)

        if not kwargs:
            kwargs = self._es_args(resource)

        kwargs['body'] = properties

        if force_index:
            kwargs['index'] = force_index

        if not es:
            es = self.elastic(resource)

        try:
            es.indices.put_mapping(**kwargs)
        except elasticsearch.exceptions.RequestError:
            logger.exception('mapping error, updating settings resource=%s' % resource)

    def _get_mapping_properties(self, resource_config, parent=None):
        properties = self._get_mapping(resource_config['schema'])
        properties['properties'].update({
            config.DATE_CREATED: self._get_field_mapping({'type': 'datetime'}),
            config.LAST_UPDATED: self._get_field_mapping({'type': 'datetime'}),
        })

        if parent:
            properties.update({
                '_parent': {
                    'type': parent.get('type')
                }
            })

        properties['properties'].pop('_id', None)
        return properties

    def put_mapping(self, app, index=None):
        """Put mapping for elasticsearch for current schema.

        It's not called automatically now, but rather left for user to call it whenever it makes sense.
        """
        for resource, resource_config in self._get_elastic_resources().items():
            datasource = resource_config.get('datasource', {})

            if not is_elastic(datasource):
                continue

            if datasource.get('source', resource) != resource:  # only put mapping for core types
                continue

            properties = self._get_mapping_properties(resource_config)

            kwargs = {
                'index': index or self._resource_index(resource),
                'doc_type': 'doc',
                'body': properties,
            }

            try:
                self.elastic(resource).indices.put_mapping(**kwargs)
            except elasticsearch.exceptions.RequestError:
                logger.exception('mapping error, updating settings resource=%s' % resource)

    def get_mapping(self, index=None, doc_type=None):
        """Get mapping for index.

        :param index: index name
        """

        mapping = self.es.indices.get_mapping(index=self._get_index_prefix(index), doc_type=doc_type)
        if index is None:
            return mapping
        return next(iter(mapping.values()))

    def get_settings(self, index):
        """Get settings for index.

        :param index: index name
        """
        index = self._get_index_prefix(index)

        settings = self.es.indices.get_settings(index=index)
        return next(iter(settings.values()))

    def get_index_by_alias(self, alias):
        """Get index name for given alias.

        If there is no alias assume it's an index.

        :param alias: alias name
        """
        try:
            info = self.es.indices.get_alias(name=alias)
            return next(iter(info.keys()))
        except elasticsearch.exceptions.NotFoundError:
            return alias

    def find(self, resource, req, sub_resource_lookup):
        """Find documents for resource."""

        args = getattr(req, 'args', request.args if request else {}) or {}
        source_config = config.SOURCES[resource]

        source_filter = None
        if args.get('source'):
            query = json.loads(args.get('source'))
            if 'filter' in query.get('query', {}):
                _query = query.get('query')
                query['query'] = {'bool': {}}
                if _query:
                    source_filter = _query['filter']
            else:
                _query = query.get('query')
                query = {'query': {'bool': {}}}
                source_filter = _query

        else:
            query = {'query': {'bool': {}}}

        must_filter = []
        if args.get('q', None):
            # underscore is not supported/ignored in es 6.2, use -all- instead where we copied text and keyword to.
            must_filter.append(_build_query_string(args.get('q'),
                                                   default_field=args.get('df', 'all'),
                                                   default_operator=args.get('default_operator', 'OR')))

        if 'sort' not in query:
            if req.sort:
                sort = ast.literal_eval(req.sort)
                set_sort(query, sort)
            elif self._default_sort(resource):
                set_sort(query, self._default_sort(resource))

        if req.max_results:
            query.setdefault('size', req.max_results)

        if req.page > 1:
            query.setdefault('from', (req.page - 1) * req.max_results)

        filters = []
        filters.append(source_config.get('elastic_filter'))
        filters.append(source_config.get('elastic_filter_callback', noop)())
        filters.append(source_filter)

        must_filter.append(_build_lookup_filter(sub_resource_lookup) if sub_resource_lookup else None)
        must_filter.append(json.loads(args.get('filter')) if 'filter' in args else None)
        must_filter.extend(args.get('filters') if 'filters' in args else [])

        if req.where:
            try:
                filters.append({'term': json.loads(req.where)})
            except ValueError:
                try:
                    filters.append({'term': parse(req.where)})
                except ParseError:
                    abort(400)

        set_filters(query, must_filter, filters)

        if 'facets' in source_config:
            query['facets'] = source_config['facets']

        if 'aggregations' in source_config and self.should_aggregate(req):
            query['aggs'] = source_config['aggregations']

        if 'es_highlight' in source_config and self.should_highlight(req):

            query_string = query['query']
            highlights = source_config.get('es_highlight', noop)(query_string)

            if highlights:
                query['highlight'] = highlights
                query['highlight'].setdefault('require_field_match', False)

        source_projections = None
        if self.should_project(req):
            source_projections = self.get_projected_fields(req)

        args = self._es_args(resource, source_projections=source_projections)

        try:
            hits = self.elastic(resource).search(body=query, **args)
        except elasticsearch.exceptions.RequestError as e:
            if e.status_code == 400 and "No mapping found for" in e.error:
                hits = {}
            elif e.status_code == 400 and 'SearchParseException' in e.error:
                raise InvalidSearchString
            else:
                raise

        return self._parse_hits(hits, resource)

    def should_aggregate(self, req):
        """Check the environment variable and the given argument parameter to decide if aggregations needed.

        argument value is expected to be '0' or '1'
        """
        try:
            return self.app.config.get('ELASTICSEARCH_AUTO_AGGREGATIONS') or \
                   bool(req.args and int(req.args.get('aggregations')))
        except Exception:
            return False

    def should_highlight(self, req):
        """Check the given argument parameter to decide if highlights needed.

        argument value is expected to be '0' or '1'
        """
        try:
            return bool(req.args and int(req.args.get('es_highlight', 0)))
        except Exception:
            return False

    def should_project(self, req):
        """Check the given argument parameter to decide if projections needed.

        argument value is expected to be a list of strings
        """
        try:
            return req.args and json.loads(req.args.get('projections', []))
        except Exception:
            return False

    def get_projected_fields(self, req):
        """Return the projected fields from request."""

        try:
            args = getattr(req, 'args', {})
            return ','.join(json.loads(args.get('projections')))
        except Exception:
            return None

    def find_one(self, resource, req, **lookup):
        """Find single document, if there is _id in lookup use that, otherwise filter."""

        if config.ID_FIELD in lookup:
            return self._find_by_id(resource=resource, _id=lookup[config.ID_FIELD], parent=lookup.get('parent'))
        else:
            args = self._es_args(resource)

            filters = [{'term': {key: val}} for key, val in lookup.items()]
            query = {'query': {'bool': {'filter': filters}}}

            try:
                args['size'] = 1
                hits = self.elastic(self._get_index_prefix(resource)).search(body=query, **args)
                docs = self._parse_hits(hits, resource)
                return docs.first()
            except elasticsearch.NotFoundError:
                return

    def _find_by_id(self, resource, _id, parent=None):
        """Find the document by Id.

        If parent is not provided then on routing exception try to find using search.
        """
        def is_found(hit):
            if 'exists' in hit:
                hit['found'] = hit['exists']
            return hit.get('found', False)

        args = self._es_args(resource)
        try:
            # set the parent if available
            if parent:
                args['parent'] = parent

            hit = self.elastic(resource).get(id=_id, **args)

            if not is_found(hit):
                return

            docs = self._parse_hits({'hits': {'hits': [hit]}}, resource)
            return docs.first()

        except elasticsearch.NotFoundError:
            return
        except elasticsearch.TransportError as tex:
            if tex.error == 'routing_missing_exception' or 'RoutingMissingException' in tex.error:
                # search for the item
                args = self._es_args(resource)
                query = {'query': {'bool': {'must': [{'term': {'_id': _id}}]}}}
                try:
                    args['size'] = 1
                    hits = self.elastic(resource).search(body=query, **args)
                    docs = self._parse_hits(hits, resource)
                    return docs.first()
                except elasticsearch.NotFoundError:
                    return

    def find_one_raw(self, resource, _id):
        """Find document by id."""
        return self._find_by_id(resource=resource, _id=_id)

    def find_list_of_ids(self, resource, ids, client_projection=None):
        """Find documents by ids."""
        args = self._es_args(resource)
        return self._parse_hits(self.elastic(resource).mget(body={'ids': ids}, **args), resource)

    def insert(self, resource, doc_or_docs, **kwargs):
        """Insert document, it must be new if there is ``_id`` in it."""
        ids = []
        kwargs.update(self._es_args(resource))

        for doc in doc_or_docs:
            self._update_parent_join_args(kwargs, doc)
            _id = doc.pop('_id', None)
            res = self.elastic(resource).index(body=doc, id=_id, **kwargs)
            doc.setdefault('_id', res.get('_id', _id))
            ids.append(doc.get('_id'))
        self._refresh_resource_index(resource)
        return ids

    def bulk_insert(self, resource, docs, **kwargs):
        """Bulk insert documents.

        If documents contain join_fields a field _routing containing
        the parent_id has to be added to make sure parent-join works as they have to be indexed on
        the same shard.
        """

        kwargs.update(self._es_args(resource))

        # if a join field exists a routing has to be added, see test_bulk_insert for example
        res = bulk(self.elastic(resource), docs, stats_only=False, **kwargs)
        self._refresh_resource_index(resource)
        return res

    def update(self, resource, id_, updates, original=None):
        """Update document in index."""
        args = self._es_args(resource, refresh=True)
        if self._get_retry_on_conflict():
            args['retry_on_conflict'] = self._get_retry_on_conflict()
        updates.pop('_id', None)
        updates.pop('_type', None)
        self._update_parent_join_args(args, updates)
        return self.elastic(resource).update(id=id_, body={'doc': updates}, **args)

    def replace(self, resource, id_, document):
        """Replace document in index."""
        args = self._es_args(resource, refresh=True)
        document.pop('_id', None)
        document.pop('_type', None)
        self._update_parent_join_args(args, document)
        return self.elastic(resource).index(body=document, id=id_, **args)

    def remove(self, resource, lookup=None, parent=None, **kwargs):
        """Remove docs for resource.

        :param resource: resource name
        :param lookup: filter
        :param parent: parent id
        """
        kwargs.update(self._es_args(resource))
        if parent:
            kwargs['parent'] = parent
        if lookup:
            if lookup.get('_id'):
                try:
                    return self.elastic(resource).delete(id=lookup.get('_id'), refresh=True, **kwargs)
                except elasticsearch.NotFoundError:
                    return
        return ValueError('there must be `lookup._id` specified')

    def is_empty(self, resource):
        """Test if there is no document for resource.

        :param resource: resource name
        """
        resource = self._get_index_prefix(resource)
        args = self._es_args(resource)
        res = self.elastic(resource).count(body={'query': {'match_all': {}}}, **args)
        return res.get('count', 0) == 0

    def put_settings(self, app=None, index=None, settings=None, es=None):
        """Modify index settings.

        Index must exist already.
        """
        if not index:
            index = self.index

        if not app:
            app = self.app

        if not es:
            es = self.es

        if not settings:
            return

        es.indices.close(index=index)
        es.indices.put_settings(index=index, body=settings)
        es.indices.open(index=index)

    def _parse_hits(self, hits, resource):
        """Parse hits response into documents."""
        datasource = self.get_datasource(resource)
        schema = {}
        schema.update(config.DOMAIN[datasource[0]].get('schema', {}))
        schema.update(config.DOMAIN[resource].get('schema', {}))
        dates = get_dates(schema)
        docs = []
        for hit in hits.get('hits', {}).get('hits', []):
            docs.append(format_doc(hit, schema, dates))
        return ElasticCursor(hits, docs)

    def _es_args(self, resource, refresh=None, source_projections=None):
        """Get index and doctype args."""
        # doc type name will be obsolete in the future but not yet updated in elasticsearch-py
        # right now it will be always the only mapping type which is called doc
        # https://github.com/elastic/elasticsearch-py/issues/646
        args = {
            'index': self._resource_index(resource),
            'doc_type': "doc",
        }
        if source_projections:
            args['_source'] = source_projections
        if refresh:
            args['refresh'] = refresh

        return args

    def _get_parent_type(self, resource):
        resource_config = self.app.config['DOMAIN'][resource] or {}
        return resource_config.get('schema', {}).get('elastic_parent', {})

    def get_parent_id(self, document):
        """Get the parent id for routing the child to the proper shard.

        :param document: document containing the parent id
        """
        join_field = document.get('join_field')

        if join_field and 'parent' in join_field:
            return join_field.get('parent')

        return None

    def _update_parent_join_args(self, args, document):
        """Add routing to arguments when document contains a parent field."""
        routing = self.get_parent_id(document)
        if routing:
            args['routing'] = routing

    def _fields(self, resource):
        """Get projection fields for given resource."""
        datasource = self.get_datasource(resource)
        keys = datasource[2].keys()
        return ','.join(keys) + ','.join([config.LAST_UPDATED, config.DATE_CREATED])

    def _default_sort(self, resource):
        datasource = self.get_datasource(resource)
        return datasource[3]

    def _resource_index(self, resource):
        """Get index for given resource.

        by default it will be `self.index`, but it can be overriden via app.config

        :param resource: resource name
        """
        datasource = self.get_datasource(resource)
        indexes = self._resource_config(resource, 'INDEXES') or {}
        default_index = self._get_index_prefix(resource)
        return indexes.get(datasource[0], default_index)

    def _refresh_resource_index(self, resource):
        """Refresh index for given resource.

        :param resource: resource name
        """
        if self._resource_config(resource, 'FORCE_REFRESH', True):
            self.elastic(resource).indices.refresh(self._resource_index(resource))

    def _resource_prefix(self, resource=None):
        """Get elastic prefix for given resource.

        Resource can specify ``elastic_prefix`` which behaves same like ``mongo_prefix``.
        """
        px = 'ELASTICSEARCH'
        if self.app.config['ELASTICSEARCH_INDEX_PREFIX']:
            resource = resource.replace(self.app.config['ELASTICSEARCH_INDEX_PREFIX'],'')
        if resource and config.DOMAIN[resource].get('elastic_prefix'):
            px = config.DOMAIN[resource].get('elastic_prefix')
        return px

    def _resource_config(self, resource=None, key=None, default=None):
        """Get config using resource elastic prefix (if any)."""
        px = self._resource_prefix(resource)
        return self.app.config.get('%s_%s' % (px, key), default)

    def _get_index_prefix(self, index=None):
        """Get prefix that should be added to each index.

        This allows to save indexes with a seperate prefix in elasticsearch,
        however entry point and request stay the same. So the name of index is
        different in elasticsearch itself and the layer handles the querying for
        the right index.

        """

        if index and self.app.config['ELASTICSEARCH_INDEX_PREFIX']:
            prefixed_index = self.app.config['ELASTICSEARCH_INDEX_PREFIX'] + index
            return prefixed_index
        else:
            return index


    def elastic(self, resource=None):
        """Get ElasticSearch instance for given resource."""
        px = self._resource_prefix(resource)

        if px not in self.elastics:
            url = self._resource_config(resource, 'URL')
            assert url, 'no url for %s' % px
            self.elastics[px] = get_es(url, **self.kwargs)

        return self.elastics[px]

    def _get_retry_on_conflict(self):
        """Get the retry on settings."""
        return self.app.config.get('ELASTICSEARCH_RETRY_ON_CONFLICT', 5)


def build_elastic_query(doc):
    """Build a query which follows ElasticSearch syntax from doc.

    1. Converts {"q":"cricket"} to the below elastic query::

        {
            "query": {
                        "query_string": {
                            "query": "cricket",
                            "lenient": false,
                            "default_operator": "AND"
                }
            }
        }

    2. Converts a faceted query::

        {"q":"cricket", "type":['text'], "source": "AAP"}

    to the below elastic query::

        {
            "query": {
                "bool": {
                    "filter": {
                        "and": [
                            {"terms": {"type": ["text"]}},
                            {"term": {"source": "AAP"}}
                        ]
                    },
                    "query": {
                        "query_string": {
                            "query": "cricket",
                            "lenient": false,
                            "default_operator": "AND"
                        }
                    }
                }
            }
        }

    :param doc: A document object which is inline with the syntax specified in the examples.
                It's the developer responsibility to pass right object.
    :returns ElasticSearch query
    """
    elastic_query, filters = {"query": {}}, []

    for key in doc.keys():
        if key == 'q':
            elastic_query['query'] = _build_query_string(doc['q'])
        else:
            _value = doc[key]
            filters.append({"terms": {key: _value}} if isinstance(_value, list) else {"term": {key: _value}})

    set_filters(elastic_query, filters)
    return elastic_query


def _build_query_string(q, default_field=None, default_operator='AND'):
    """Build ``query_string`` object from ``q``.

    :param q: q of type String
    :param default_field: default_field
    :return: dictionary object.
    """
    def _is_phrase_search(query_string):
        clean_query = query_string.strip()
        return clean_query and clean_query.startswith('"') and clean_query.endswith('"')

    def _get_phrase(query_string):
        return query_string.strip().strip('"')

    if _is_phrase_search(q):
        query = {'match_phrase': {'all': _get_phrase(q)}}
    else:
        query = {'query_string': {'query': q, 'default_operator': default_operator}}
        # query['query_string'].update({'lenient': False} if default_field else {'default_field': default_field})

    return query


def _build_lookup_filter(lookup):
    return [{'term': {key: val}} for key, val in lookup.items()]
