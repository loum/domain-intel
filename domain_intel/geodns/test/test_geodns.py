""":class:`domain_intel.geodns.GeoDNSStage` unit test cases.

"""
import os
import time
import mock
import pytest

from domain_intel import geodns
from domain_intel.geodns import CheckHostNetError, WorkerTimedOut

geodns_sample_dir = os.path.join(
    'domain_intel',
    'geodns',
    'test',
    'files',
    'samples',
    'geodns',
)

with open(os.path.join(geodns_sample_dir, "resolve_compass.out"), "rb") as _fh:
    _resolve_compass_res = _fh.read()
with open(os.path.join(geodns_sample_dir, "resolve_dns.out"), "rb") as _fh:
    _resolve_dns_res = geodns.CheckHostNetResult.from_serialised(_fh.read())
with open(os.path.join(geodns_sample_dir, "parsed_dns.out"), "rb") as _fh:
    _parsed_dns_res = _fh.read()

@pytest.fixture(scope='session')
def staged_add_domains_geodns():
    """Load GTR domains into 'dns-domains' Kafka topic"""

    stage1 = geodns.GeoDNSStage(
        kafka_producer_topics=["dns-domains"],
    )
    domain_file = os.path.join(
        'domain_intel',
        'test',
        'files',
        'samples',
        'gtr_unique_domains.csv'
    )

    def _chomped_lines(_filename):
        with open(_filename, "rb") as _fh:
            for line in _fh:
                yield line.rstrip()

    metrics = stage1.publish(_chomped_lines(domain_file))

    return metrics["dns-domains"]

@pytest.mark.usefixtures('docker_compose')
def test_staged_add_domains_geodns(staged_add_domains_geodns):
    """realise the add_domains_geodns fixture, make sure it added the 10000 static domains"""
    assert staged_add_domains_geodns == 1000, 'Load of GTR domains into Kafka error'

@pytest.fixture(scope='session')
@mock.patch('domain_intel.geodns.CheckHostNet.resolve_dns')
def staged_resolve_domains_geodns(mock_resolve_dns):
    # given a valid checkhostnet raw result
    mock_resolve_dns.return_value = _resolve_dns_res

    # first stage of pipeline (dns-domain to dns-raw) should run
    stage2 = geodns.GeoDNSStage(
        kafka_consumer_topics=["dns-domains"],
        kafka_producer_topics=["dns-raw"],
        kafka_consumer_group_id="resolve_domains_geodns",
        worker=geodns.GeoDNS(
            compass_username="",
            compass_password="",
        ).resolve_dns,
        max_read_count=10,
    )
    return stage2.run()

@pytest.mark.usefixtures(
    'docker_compose',
    'staged_add_domains_geodns',
)
@mock.patch('domain_intel.geodns.CheckHostNet.resolve_dns')
def test_staged_resolve_domains_geodns_with_exception(mock_resolve_dns):
    # given a failing checkhostnet response
    mock_resolve_dns.side_effect = CheckHostNetError("mocked exception")

    # first stage of pipeline (dns-domain to dns-raw) should error out
    stage2 = geodns.GeoDNSStage(
        kafka_consumer_topics=["dns-domains"],
        kafka_producer_topics=["dns-raw"],
        kafka_consumer_group_id="resolve_domains_geodns_withexceptions",
        worker=geodns.GeoDNS(
            compass_username="",
            compass_password="",
        ).resolve_dns,
        retryable_exceptions=(CheckHostNetError,),
        retryable_exceptions_count=2,
        max_read_count=10,
    )
    try:
        stage2.run()
    except CheckHostNetError as exc:
        # after having tried N times
        assert stage2.metrics["retryable_exceptions"] == stage2.retryable_exceptions_count, "retried catchable exceptions"


@pytest.mark.usefixtures(
    'docker_compose',
    'staged_add_domains_geodns',
)
@mock.patch('domain_intel.geodns.CheckHostNet.resolve_dns')
def test_staged_resolve_domains_geodns_with_timeout(mock_resolve_dns):
    # given a successful but slow checkhostnet response
    mock_resolve_dns.side_effect = lambda *args, **kwargs: time.sleep(9999)
    mock_resolve_dns.return_value = _resolve_dns_res

    # first stage of pipeline (dns-domain to dns-raw) should error out
    stage2 = geodns.GeoDNSStage(
        kafka_consumer_topics=["dns-domains"],
        kafka_producer_topics=["dns-raw"],
        kafka_consumer_group_id="resolve_domains_geodns_withtimeout",
        worker=geodns.GeoDNS(
            compass_username="",
            compass_password="",
        ).resolve_dns,
        worker_timeout_seconds=5,
        retryable_exceptions=(CheckHostNetError,),
        retryable_exceptions_count=2,
        max_read_count=10,
    )
    try:
        stage2.run()
    except WorkerTimedOut as exc:
        # after having tried N times
        assert stage2.metrics["retryable_exceptions"] == stage2.retryable_exceptions_count, "retried after timeout"


@pytest.mark.usefixtures(
    'docker_compose',
    'staged_add_domains_geodns',
)
def test_staged_resolve_domains_geodns(staged_resolve_domains_geodns):
    """realise the resolve_domains_geodns fixture, ensure it processed 10 messages"""
    assert staged_resolve_domains_geodns["messages_received"] == staged_resolve_domains_geodns["messages_processed"] == 10, "consumer resolved dns for 10 domains"


@pytest.fixture(scope='session')
def staged_parse_checkhostnetresult_geodns():
    stage3 = geodns.GeoDNSStage(
        kafka_consumer_topics=["dns-raw"],
        kafka_producer_topics=["dns-parsed"],
        kafka_consumer_group_id="parse_checkhostnetresult",
        worker=geodns.GeoDNS(
            compass_username="",
            compass_password="",
        ).parse_checkhostnetresult,
        max_read_count=10,
    )
    return stage3.run()


@pytest.mark.usefixtures(
    'docker_compose',
    'staged_add_domains_geodns',
    'staged_resolve_domains_geodns',
)
def test_staged_parse_checkhostnetresult(staged_parse_checkhostnetresult_geodns):
    """realise the staged_parse_checkhostnetresult_geodns fixture, ensure it processed 10 messages"""
    assert staged_parse_checkhostnetresult_geodns["messages_received"] == staged_parse_checkhostnetresult_geodns["messages_processed"] == 10, "consumer resolved dns for 10 domains"

@pytest.fixture(scope='session')
@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def staged_resolve_geog_from_dns_geodns(mock_compass_post):
    # return the same compass result each time
    mock_compass_post.return_value.content = _resolve_compass_res
    mock_compass_post.return_value.status_coode = 200

    stage4 = geodns.GeoDNSStage(
        kafka_consumer_topics=["dns-parsed"],
        kafka_producer_topics=["dns-geodns-parsed"],
        kafka_consumer_group_id="resolve_geog_from_dns",
        worker=geodns.GeoDNS(
            compass_username="",
            compass_password="",
        ).resolve_geog_from_dns,
        max_read_count=10,
    )
    return stage4.run()

@pytest.mark.usefixtures(
    'docker_compose',
    'staged_add_domains_geodns',
    'staged_resolve_domains_geodns',
    'staged_parse_checkhostnetresult_geodns',
)
def test_staged_resolve_geog_from_dns(staged_resolve_geog_from_dns_geodns):
    """realise the staged_resolve_geog_from_dns_geodns fixture, ensure it processed 10 messages"""
    assert staged_resolve_geog_from_dns_geodns["messages_received"] == staged_resolve_geog_from_dns_geodns["messages_processed"] == 10, "consumer resolved dns for 10 domains"

def test_geodns_parse_dns():
    """test we can parse and restructure a valid dns output"""

    worker =  geodns.GeoDNS(
        compass_username="",
        compass_password="",
        dns_resolver=geodns.CheckHostNet(),
    )

    domain = "www.ip-echelon.com"
    parsed_response = worker.parse_checkhostnetresult(_resolve_dns_res)

    #TODO: not sure what to assert here, its a big dict?
    assert parsed_response is not None, "can parse valid dns results"


@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def test_geodns_geog_from_dns(mock_compass_post):

    # return the same compass result each time
    mock_compass_post.return_value.content = _resolve_compass_res
    mock_compass_post.return_value.status_coode = 200

    # TODO: fixtures for this object
    worker = geodns.GeoDNS(
        compass_username="",
        compass_password="",
        dns_resolver=geodns.CheckHostNet(),
    )

    # directly instantiate a parsed response
    parsed_dns_results = worker.parse_checkhostnetresult(_resolve_dns_res)

    parsed_geog_results = worker.resolve_geog_from_dns(parsed_dns_results)
    # TODO: not sure what to assert for this
    assert parsed_geog_results is not None, "resolve_geog_from_dns(valid_dns_results) returns something"

@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def test_resolved_geog_from_dns_geodns_empty(mock_compass_post):
    """test resolution from valid dns results to empty geog results"""

    mock_compass_post.return_value.content = b'{"Error":"no routes"}\n'
    mock_compass_post.return_value.status_code = 500

    worker = geodns.GeoDNS(
        compass_username=":",
        compass_password="",
        dns_resolver=geodns.CheckHostNet(),
    )

    # directly instantiate a parsed response
    parsed_geog_results = worker.resolve_geog_from_dns(_parsed_dns_res)
    assert parsed_geog_results is not None, "resolve_geog_from_dns(valid_dns_results) returns something"
    assert len(parsed_geog_results["geog_results"].keys()) == 0, "missing compass data should give geog_results with 0 keys"


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'reload_flattened_geodns',
                         'init_arango')
def test_persist(persist_geodns):
    """Persist GeoDNS data to ArangoDB.
    """
    # ... then I should match the count of items persisted
    msg = 'ArangoDB "geodns" insert count error'
    # combination of this test and staged_resolve_geog_from_dns_geodns
    assert persist_geodns in (79, 89), msg
