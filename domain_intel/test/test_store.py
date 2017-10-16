""":class:`domain_intel.Store` unit test cases.

"""
import pytest

import domain_intel


def test_store_init():
    """Initialise a domain_intel.Awis object.
    """
    # When I initialise a Domain Intel Store object
    store = domain_intel.Store()

    # I should get a domain_intel.Store instance
    msg = 'Object is not a domain_intel.Store instance'
    assert isinstance(store, domain_intel.Store), msg


@pytest.mark.usefixtures('docker_compose')
def test_initialise_already_exists(arango_ready):
    """Initialise an ArangoDB database with a database that already exists.
    """
    # Given a database name
    db_name = 'my_database'

    # that already exists
    arango_ready(db_name)

    # when I re-initialise a database
    store = domain_intel.Store(db_name)
    received = store.initialise()

    # then the database should not be recreated
    msg = 'Recreating existing database should produce empty list'
    assert not received, msg


@pytest.mark.usefixtures('docker_compose')
def test_build_graph_collection(arango_ready):
    """Build the graph collections.
    """
    # Given a database name
    db_name = 'test_database'

    # when I initialise a graph collection
    received = arango_ready(db_name)

    # then I should receive a list of collections
    msg = 'Collection count error'
    expected = [
        'domain',
        'url-info',
        'geodns',
        'country',
        'link',
        'subdomain',
        'url',
        'ipv4',
        'ipv6',
        'traffic',
        'analyst-qas',
    ]
    assert sorted([x.name for x in received]) == sorted(expected), msg


@pytest.mark.usefixtures('docker_compose')
def test_re_build_graph_collection(arango_ready):
    """Re-build the graph collections.
    """
    # Given a database name
    db_name = 'test_database'

    # when I initialise a graph collection
    arango_ready(db_name)

    # and re-initialise a graph collection
    store = domain_intel.Store(db_name)
    store.version()
    store.initialise()
    received = store.build_graph_collection()

    # then I should receive an empty list of collections
    msg = 'Re-build collection count error'
    assert not received, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_persist_country_codes(persist_country):
    """Persist country codes.
    """
    msg = 'Count against the country collection incorrect'
    assert persist_country == 247, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'parse_raw_siteslinkingin',
                         'persist_siteslinkingin',
                         'reload_flattened_geodns',
                         'persist_geodns',
                         'parse_raw_traffichistory',
                         'flatten_traffic',
                         'persist_traffichistory',
                         'parse_qas',
                         'persist_analystqas')
def test_traverse_graph():
    """Traverse the graph relationship.
    """
    # Given a domain label
    label = 'domain/ondertitel.com'

    # when I traverse the graph
    store = domain_intel.Store()
    received = store.traverse_graph(label, as_json=True)

    # then I should receive a result
    msg = 'Valid label graph traversal did not return result'
    assert received is not None, msg
