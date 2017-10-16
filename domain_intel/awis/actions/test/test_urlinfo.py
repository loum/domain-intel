""":class:`domain_intel.AlexaApi` unit test cases.

"""
import os
import calendar
from datetime import datetime
import pytest
import mock

import domain_intel.awis.actions
import domain_intel.utils


def test_awis_urlinfo_init():
    """Initialise a domain_intel.awis.actions.UrlInfo object.
    """
    # When I initialise an Alexa Web Information object
    awis = domain_intel.awis.actions.UrlInfo()

    # I should get a domain_intel.awis.actions.UrlInfo instance
    msg = 'Object is not a domain_intel.awis.actions.UrlInfo instance'
    assert isinstance(awis, domain_intel.awis.actions.UrlInfo), msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_add_domains(add_domains):
    """Load of GTR domains into Kafka topic.
    """
    msg = 'Load of GTR domains into Kafka error'
    assert add_domains == 1000, msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready', 'add_domains')
def test_read_domains(read_domains):
    """Read GTR domains from Kafka topic.
    """
    msg = 'Read GTR domains from Kafka error'
    assert read_domains == 1000, msg


def test_flatten_batched_domain_xml():
    """Flatten a batched-domain XML response from Alexa AWIS.
    """
    # Given an Alexa AWIS batched XML response
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'multi-domain-result.xml')) as _fh:
        source_xml = _fh.read().rstrip()

    # when I flatten the XML
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.flatten_batched_xml(source_xml)

    # then I should receive a list of JSON-encode domains
    msg = 'Flattened domains should produce list of JSON-encoded domains'
    assert len(received) == 2, msg


def test_flatten_single_domain_xml():
    """Flatten a single-domain XML response from Alexa AWIS.
    """
    # Given an Alexa AWIS batched XML response
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'single-domain-result.xml')) as _fh:
        source_xml = _fh.read().rstrip()

    # when I flatten the XML
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.flatten_batched_xml(source_xml)

    # then I should receive a list of JSON-encode domains
    msg = 'Flattened domain should produce JSON-encoded domain list'
    assert len(received) == 1, msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_parse_raw_urlinfo(parse_raw_urlinfo):
    """Parse raw Alexa XML.
    """
    msg = 'Real load of raw Alexa load into Kafka error'
    assert parse_raw_urlinfo == (9385, 16, 16), msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo')
def test_flatten_domains(flatten_domains):
    """Flatten batched Alexa XML and publish.
    """
    msg = 'Flattening batched Alexa XML back into Kafka error'
    assert flatten_domains == 16, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango')
def test_persist(persist_domains):
    """Persist domain to the database.
    """
    # ... then I should receive a record count if domains stored
    msg = 'Count against the domain collection incorrect'
    assert persist_domains == (80, 80), msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains')
def test_alexa_csv_dump():
    """Alexa CSV dump.
    """
    # Given a unique group ID that ensure isolated consumption of messages
    group_id = domain_intel.utils.id_generator()

    # ehen I dump the flattened Alexa JSON to CSV
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.alexa_csv_dump(max_read_count=7,
                                   group_id=group_id,
                                   dry=True)

    # then I should receive a record count of 7
    msg = 'Count against the flattened Alexa JSON read is incorrect'
    assert received == 7, msg


@mock.patch('domain_intel.awis.actions.urlinfo.time')
def test_alexa_flattened_extract(mock_dt):
    """Rules to extract fields from flattened Alexa.
    """
    # Given a source of flattened Alexa JSON
    alexa_json_file = os.path.join('domain_intel',
                                   'test',
                                   'files',
                                   'samples',
                                   'flattened_alexa.json')
    # when I parse the JSON
    mock_epoch = datetime(2017, 5, 16).utctimetuple()
    mock_dt.time.return_value = calendar.timegm(mock_epoch)
    received = []
    with open(alexa_json_file) as _fh:
        for line in _fh:
            stats = domain_intel.awis.actions.UrlInfo.alexa_flattened_extract(line)
            received.append(stats)

    # then I should receive domain name ranks
    msg = 'CSV dump extraction error: domains processed'
    expected = [
        'partymegapop.info',
        'new-rutor.org.pl',
        'watchseriesonline.io',
        'kibergrad.com',
        'darktorrent.pl',
        'kickassproxy.club',
        'inevil.com',
        'partymegapop.info',
    ]
    assert [x[0][0] for x in received] == expected, msg

    msg = 'CSV dump extraction error: exported row'
    expected = ['watchseriesonline.io', 1494892800.0, 588080]
    assert received[2][0] == expected, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_write_to_store():
    """Persistent store abstraction.
    """
    # Given an Alexa UrlInfo record
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        record = _fh.read().rstrip().encode('utf')

    # when I persist the record
    awis = domain_intel.awis.actions.UrlInfo()
    awis.write_to_store(record, dry=False)

    # then I should receive a record count of 1 domain
    msg = 'Count against the domain collection incorrect'
    assert awis.store.get_collection_count() == 1, msg

    # and I should receive a record count of 1 url_info records
    msg = 'Count against the url_info collection incorrect'
    assert awis.store.get_collection_count(collection_name='url-info') == 1, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_persist_country_rank():
    """Persistent country rank abstraction.
    """
    # Given an Alexa UrlInfo record
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        record = _fh.read().rstrip().encode('utf')
    url_info = domain_intel.awisapi.parser.UrlInfo(record)

    # when I persist the country ranks
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.persist_country_rank(url_info, dry=False)

    # then I should receive a record count of 16 countries
    msg = 'Country rank return count error'
    assert received == 16, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_persist_related_links():
    """Persistent related links abstraction.
    """
    # Given an Alexa UrlInfo record
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        record = _fh.read().rstrip().encode('utf')
    url_info = domain_intel.awisapi.parser.UrlInfo(record)

    # when I persist the country ranks
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.persist_related_links(url_info, dry=False)

    # then I should receive a record count of 10 links
    msg = 'Related links return count error'
    assert received == 10, msg

    msg = 'Count against the country collection incorrect'
    assert awis.store.get_collection_count(collection_name='link') == 10, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_contributing_subdomains():
    """Persistent contributing subdomains abstraction.
    """
    # Given an Alexa UrlInfo record
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        record = _fh.read().rstrip().encode('utf')
    url_info = domain_intel.awisapi.parser.UrlInfo(record)

    # when I persist the country ranks
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.persist_contributing_subdomains(url_info,
                                                    dry=False)

    # then I should receive a record count of 7 contributing subdomains
    msg = 'Contributing subdomains links return count error'
    assert received == 7, msg

    msg = 'Count against the subdomain collection incorrect'
    assert awis.store.get_collection_count(collection_name='subdomain') == 7, msg


@pytest.mark.usefixtures('docker_compose', 'init_arango')
def test_persist_url_info():
    """Persistent URL info JSON records.
    """
    # Given an Alexa UrlInfo record
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        record = _fh.read().rstrip().encode('utf')
    url_info = domain_intel.awisapi.parser.UrlInfo(record)

    # when I persist the country ranks
    awis = domain_intel.awis.actions.UrlInfo()
    awis.persist_url_info(url_info, dry=False)
    received = awis.store.get_collection_count(collection_name='url-info')

    # then I should receive a record count of 1 URL info JSON
    msg = 'Count against the url-info'
    assert received == 1, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'add_domain_labels')
def test_traverse_relationships(traverse_relationships):
    """Traverse relationships.
    """
    # ... then I should get a count of relationship JSON structures
    msg = 'Graph relationship count incorrect'
    assert traverse_relationships == 80, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'add_domain_labels',
                         'traverse_relationships')
def test_wide_column_dump_worker():
    """Dump the wide-column CSV format.
    """
    # When I dump the wide-column CSV
    awis = domain_intel.awis.actions.UrlInfo()
    received = awis.wide_column_dump()

    # then I should get a count of CSV lines
    msg = 'Wide-column CSV count incorrect'
    assert received == (80, 242), msg
