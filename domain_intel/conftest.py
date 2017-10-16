"""Global fixture arrangment at the `domain_intel` package level

"""
import os
import pytest
import docker
import compose.cli.main
from logga import log

import domain_intel.analyst
import domain_intel.awis.actions
import domain_intel.utils
import domain_intel.geodns

CONFIG = domain_intel.common.CONFIG


@pytest.fixture(scope='session')
def add_domains():
    """Load GTR domains into the 'gtr-domains' Kafka topic.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    domains_file = os.path.join('domain_intel',
                                'test',
                                'files',
                                'samples',
                                'gtr_unique_domains.csv')
    with open(domains_file) as _fh:
        metrics = awis.add_domains(_fh)

    return metrics


@pytest.fixture(scope='session')
def add_siteslinkingin_domains():
    """Load SitesLinkingIn domains into the 'sli-domains' Kafka topic.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    domains_file = os.path.join('domain_intel',
                                'test',
                                'files',
                                'samples',
                                'siteslinkingin_domains.csv')
    with open(domains_file) as _fh:
        metrics = awis.add_domains(_fh, topic='sli-domains')

    return metrics


@pytest.fixture(scope='session')
def parse_raw_qas():
    """Load Analyst QAs into the 'analyst-qas' Kafka topic.
    """
    qas = domain_intel.analyst.Qas()
    qas_file = os.path.join('domain_intel',
                            'test',
                            'files',
                            'samples',
                            'dis_analyst_top_200.xls')
    metrics = qas.add_raw_qas(qas_file, topic='analyst-qas')

    return metrics


@pytest.fixture(scope='session')
def parse_qas():
    """Load JSON Analyst QAs into the 'analyst-qas' Kafka topic.
    """
    qas = domain_intel.analyst.Qas()
    qas_file = os.path.join('domain_intel',
                            'test',
                            'files',
                            'samples',
                            'dis_analyst_top_200.json')
    metrics = qas.add_qas(qas_file, topic='analyst-qas')

    return metrics


@pytest.fixture(scope='session')
def add_traffic_domains():
    """Load TrafficHistory domains into the 'traffic-domains' Kafka topic.
    """
    awis = domain_intel.awis.actions.TrafficHistory()
    domains_file = os.path.join('domain_intel',
                                'test',
                                'files',
                                'samples',
                                'siteslinkingin_domains.csv')
    with open(domains_file) as _fh:
        metrics = awis.add_domains(_fh, topic='traffic-domains')

    return metrics


@pytest.fixture()
def read_domains():
    """Read GTR domains from the 'gtr-domains' Kafka topic.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.read_domains(**kwargs)


@pytest.fixture(scope='session')
def parse_raw_urlinfo():
    """Load Alexa XML into the 'alexa-results' Kafka topic.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    raw_xml_file = os.path.join('domain_intel',
                                'test',
                                'files',
                                'samples',
                                'real-raw-alexa-domains.out')
    with open(raw_xml_file) as _fh:
        load_metrics = awis.parse_raw_alexa(_fh,
                                            topic='alexa-results',
                                            end_token='UrlInfoResponse')

    return load_metrics


@pytest.fixture(scope='session')
def parse_raw_traffichistory():
    """Load Alexa XML into the 'alexa-traffic-results' Kafka topic.
    """
    awis = domain_intel.awis.actions.TrafficHistory()
    raw_xml_file = os.path.join('domain_intel',
                                'test',
                                'files',
                                'samples',
                                'real_raw_alexa_traffichistory.out')
    with open(raw_xml_file) as _fh:
        load_metrics = awis.parse_raw_alexa(_fh,
                                            topic='alexa-traffic-results',
                                            end_token='TrafficHistoryResponse')

    return load_metrics


@pytest.fixture(scope='session')
def reload_flattened_geodns():
    """Load flattened GeoDNS into the 'dns-geodns-parsed' Kafka topic.
    """
    # Given a a target Kafka topic
    topic = 'dns-geodns-parsed'

    # and a source directory
    target_dir = os.path.join('domain_intel',
                              'test',
                              'files',
                              'samples',
                              'geodns_flattened')

    # when I load data into the 'dns-geodns-parsed' Kafka topic
    awis = domain_intel.Awis()
    load_metrics = awis.reload_topic(target_dir, topic)

    return load_metrics


@pytest.fixture(scope='session')
def parse_raw_siteslinkingin():
    """Re-load Alexa SitesLinkingIn JSON into Kafka topic.
    """
    awis = domain_intel.awis.actions.SitesLinkingIn()
    siteslinkingin_file = os.path.join('domain_intel',
                                       'test',
                                       'files',
                                       'samples',
                                       'siteslinkingin_as_json.out')
    with open(siteslinkingin_file) as _fh:
        load_metrics = awis.parse_raw_siteslinkingin(_fh)

    return load_metrics


@pytest.fixture(scope='session')
def flatten_domains():
    """Flatten batched Alexa XML and publish to 'alexa-flattened' topic.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.flatten_domains(**kwargs)


@pytest.fixture(scope='session')
def flatten_traffic():
    """Flatten Alexa TrafficHistory XML and publish to 'alexa-traffic-flattened' topic.
    """
    awis = domain_intel.awis.actions.TrafficHistory()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.flatten(**kwargs)


@pytest.fixture(scope='function')
def init_arango(request, db_name='ipe'):
    """Initialise the the ArangoDB database.
    """
    store = domain_intel.Store()
    store.version()

    store = domain_intel.Store(db_name)
    store.initialise()
    collections = store.build_graph_collection()

    def fin():
        """Clean up.
        """
        store.drop_database()

    request.addfinalizer(fin)

    return collections


@pytest.fixture(scope='function')
def arango_ready(request):
    """Wait until the ArangoDB server is up.
    """
    def make_arango_ready(db_name='ipe'):
        """Helper arango initialiser.
        """
        return init_arango(request, db_name=db_name)

    return make_arango_ready


@pytest.fixture(scope='function')
def persist_country():
    """Persist country information.
    """
    store = domain_intel.Store()

    return store.persist_country_codes()


@pytest.fixture(scope='function')
def persist_domains():
    """Persist domains information.
    """
    persist_country()
    awis = domain_intel.awis.actions.UrlInfo()

    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.persist(**kwargs)


@pytest.fixture(scope='function')
def persist_siteslinkingin():
    """Persist SitesLinkingIn information.
    """
    awis = domain_intel.awis.actions.SitesLinkingIn()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.persist(**kwargs)


@pytest.fixture(scope='function')
def persist_traffichistory():
    """Persist TrafficHistory information.
    """
    awis = domain_intel.awis.actions.TrafficHistory()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.persist(**kwargs)


@pytest.fixture(scope='function')
def persist_analystqas():
    """Persist Analyst QAs.
    """
    awis = domain_intel.analyst.Qas()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.persist(**kwargs)


@pytest.fixture(scope='function')
def persist_geodns():
    """Persist GeoDNS information.
    """
    # Given a source Kafka topic
    topic = 'dns-geodns-parsed'

    # when I persist the messages in the ArangoDB data store
    kwargs = {
        'kafka_consumer_topics': [topic],
        'kafka_consumer_group_id': domain_intel.utils.id_generator(),
    }
    stager = domain_intel.geodns.GeoDNSStage(**kwargs)

    return stager.persist()


@pytest.fixture(scope='session')
def traverse_relationships():
    """Traverse relationships.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    kwargs = {
        'max_read_count': None,
        'group_id': domain_intel.utils.id_generator(),
    }
    return awis.traverse_relationship(**kwargs)


@pytest.fixture(scope='session')
def add_domain_labels():
    """Persist domains information.
    """
    awis = domain_intel.awis.actions.UrlInfo()
    return awis.add_domain_labels()


@pytest.fixture(scope='session')
def kafka_ready():
    """Check that the Kafka topics are online.
    """
    awis = domain_intel.Awis()
    kwargs = {'bootstrap_servers': awis.bs_servers}
    topics = domain_intel.utils.info(**kwargs)
    domain_intel.utils.stabilise_partitions(topics, **kwargs)


@pytest.fixture(scope='session')
def docker_compose(request):
    """
    :type request: _pytest.python.FixtureRequest
    """

    options = {
        '--no-deps': False,
        '--abort-on-container-exit': False,
        'SERVICE': '',
        '--remove-orphans': False,
        '--no-recreate': True,
        '--force-recreate': False,
        '--build': False,
        '--no-build': False,
        '--no-color': False,
        '--rmi': 'none',
        '--volumes': '',
        '--follow': False,
        '--timestamps': False,
        '--tail': 'all',
        '--scale': '',
        '-d': True,
    }

    docker_compose_dir = os.path.join(os.path.dirname(__file__),
                                      'test',
                                      'docker')
    with open(os.path.join(docker_compose_dir, 'test.env'), 'w') as _fh:
        topics = CONFIG.get('kafka')['topics']
        _fh.write('KAFKA_CREATE_TOPICS={}'.format(topics))
    project = compose.cli.main.project_from_options(docker_compose_dir,
                                                    options)
    cmd = compose.cli.main.TopLevelCommand(project)
    cmd.up(options)

    def fin():
        """Tear down.
        """
        cmd.down(options)
        log.info('Deleting dangling docker data containers')
        client = docker.from_env()
        client.volumes.prune()

    request.addfinalizer(fin)
