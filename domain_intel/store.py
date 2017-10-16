""":class:`Store`

"""
import json
import arango
import arango.exceptions
import requests.exceptions
import backoff
from logga import log

import domain_intel.common

CONFIG = domain_intel.common.CONFIG
COUNTRY_CODES = domain_intel.common.COUNTRY_CODES


class Store(object):
    """:class:`Store`

    ..attribute:: database_name
        name of the ArangoDB database.  Defaults to 'ipe'

    """
    def __init__(self, database_name='ipe'):
        self.__database_name = database_name

        client_kwargs = {
            'protocol': 'http',
            'host': CONFIG.get('arango_host'),
            'port': CONFIG.get('arango_port'),
            'username': CONFIG.get('arango_username'),
            'password': CONFIG.get('arango_password'),
            'enable_logging': True,
        }
        self.__client = arango.ArangoClient(**client_kwargs)
        self.__graph = None

    @property
    def database_name(self):
        """Name of the Arango database.
        """
        return self.__database_name

    @property
    def client(self):
        """Client access to ArangoDB.
        """
        return self.__client

    @property
    def graph(self):
        """Client access to ArangoDB graph.
        """
        if self.__graph is None:
            database = self.__client.db(self.database_name)
            try:
                self.__graph = database.create_graph('domain-intel')
            except arango.exceptions.GraphCreateError:
                self.__graph = database.graph('domain-intel')

        return self.__graph

    @backoff.on_exception(backoff.expo,
                          (arango.exceptions.ServerVersionError,
                           requests.exceptions.ConnectionError),
                          max_tries=15)
    def version(self):
        """Obtain ArangoDB version.

        Can also be used to verify health of system.

        """
        log.info('ArangoDB version: %s ready', self.client.version())

    def initialise(self):
        """Initialise an ArangoDB database.

        """
        dbs_created = []
        log.info('Attempting to create DB: "%s"', self.database_name)
        try:
            db_obj = self.client.create_database(self.database_name)
            dbs_created.append(db_obj.name)
        except arango.exceptions.DatabaseCreateError as err:
            log.warning('Database "%s" create error: %s',
                        self.database_name, err)

        log.info('Databases created: %s', dbs_created)

        return dbs_created

    def build_graph_collection(self):
        """Set up the IPE Domain Intel collections.

        **Returns:**
            list of <collections> created

        """
        collections = []

        collection_names = [
            'url-info',
            'geodns',
            'domain',
            'country',
            'link',
            'subdomain',
            'url',
            'ipv4',
            'ipv6',
            'traffic',
            'analyst-qas',
        ]
        for name in collection_names:
            try:
                collections.append(self.graph.create_vertex_collection(name))
            except arango.exceptions.VertexCollectionCreateError:
                pass


        edge_defs = [
            {
                'from': 'domain',
                'name': 'ranked',
                'to': 'country',
            },
            {
                'from': 'domain',
                'name': 'related',
                'to': 'link',
            },
            {
                'from': 'subdomain',
                'name': 'contribute',
                'to': 'domain',
            },
            {
                'from': 'url',
                'name': 'links_into',
                'to': 'domain',
            },
            {
                'from': 'domain',
                'name': 'ipv4_resolves',
                'to': 'ipv4',
            },
            {
                'from': 'domain',
                'name': 'ipv6_resolves',
                'to': 'ipv6',
            },
            {
                'from': 'traffic',
                'name': 'visit',
                'to': 'domain',
            },
            {
                'from': 'domain',
                'name': 'marked',
                'to': 'analyst-qas',
            },
        ]
        for edge_def in edge_defs:
            try:
                self.graph.create_edge_definition(
                    name='{}'.format(edge_def['name']),
                    from_collections=['{}'.format(edge_def['from'])],
                    to_collections=['{}'.format(edge_def['to'])],
                )
            except arango.exceptions.EdgeDefinitionCreateError:
                pass

        return collections

    def collection_insert(self, collection_name, kwargs, dry=False):
        """Insert *kwargs* into *collection_name*.

        Returns:
            Boolean try on success.  False otherwise

        """
        persist_status = False
        collection = self.graph.vertex_collection(collection_name)
        log.info('Inserting key: "%s" into collection %s',
                 kwargs.get('_key'), collection_name)
        if not dry:
            try:
                collection.insert(kwargs)
                persist_status = True
            except arango.exceptions.DocumentInsertError as err:
                log.error('%s: %s', err, kwargs)

        return persist_status

    def edge_insert(self, edge_name, kwargs, dry=False):
        """Manage an ArangoDB edge insert.

        """
        persist_status = False

        edge = self.graph.edge_collection(edge_name)
        log.info('Inserting key: "%s" into edge %s',
                 kwargs.get('_key'), edge_name)
        if not dry:
            try:
                edge.insert(kwargs)
                persist_status = True
            except arango.ArangoError as err:
                log.error('%s: %s', err, kwargs)

        return persist_status

    def get_collection_count(self, collection_name='domain'):
        """Get count of all documents in *collection_name*.

        *collection_name* is the name of the collection to source
        (defaults to *domain*).

        **Returns:**
            integer representation of document count in *collection_name*
            or `None` if an error is encountered

        """
        count = None
        database = self.client.db(self.database_name)
        collection = database.collection(collection_name)
        count = collection.count()

        return count

    def drop_database(self):
        """Remove the ArangoDB database identified by the
        :attr:`database_names`.

        """
        log.info('Deleting database "%s"', self.database_name)
        self.client.delete_database(self.database_name)

    def persist_country_codes(self, dry=False):
        """Pre-load required country code information.

        """
        collection_count = 0

        for country_code, country_name in COUNTRY_CODES.items():
            kwargs = {
                '_key': country_code.upper(),
                'name': country_name,
            }
            if self.collection_insert('country', kwargs, dry=dry):
                collection_count += 1

        return collection_count

    def export_ids(self, collection_name):
        """Dump all of the `_id` column values from *collection_name*

        Returns:
            generator object that references the label names taken
            from the *collection_name* collection

        """
        log.info('Dumping collection "%s" labels', collection_name)

        database = self.client.db(self.database_name)
        collection = database.collection(collection_name)
        cursor = collection.export(flush=True, filter_fields=['_id'])

        while True:
            value = cursor.next()
            yield value.get('_id')

    def traverse_graph(self, label, as_json=True):
        """Traverse the :attr:`graph` starting at vertex denoted by
        *label*.

        Returns:
            the graph structure as a dictionary optionally converted
            to JSON if *as_json* is set

        """
        log.debug('Traversing label "%s"', label)

        result = None
        try:
            result = self.graph.traverse(label,
                                         direction='any',
                                         max_depth=1)
        except arango.exceptions.GraphTraverseError as err:
            log.error('Label "%s" traverse error: %s', label, err)

        if result is not None and as_json:
            result = json.dumps(result)

        return result
