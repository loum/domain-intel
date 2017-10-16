""":class:`domain_intel.awis.actions.TrafficHistory` unit test cases.

"""
import os
import json
import pytest

import domain_intel.awis.actions


def test_awis_traffichistory_init():
    """Initialise a domain_intel.awis.actions.TrafficHistory object.
    """
    # When I initialise an Alexa Web Information object
    awis = domain_intel.awis.actions.TrafficHistory()

    # I should get a domain_intel.awis.actions.TrafficHistory instance
    msg = 'Object not a domain_intel.awis.actions.TrafficHistory instance'
    assert isinstance(awis, domain_intel.awis.actions.TrafficHistory), msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_add_domains(add_traffic_domains):
    """Load of TrafficHistory domains into Kafka topic.
    """
    msg = 'Load of TrafficHistory domains into Kafka error'
    assert add_traffic_domains == 100, msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_parse_raw_traffichistory(parse_raw_traffichistory):
    """Parse raw Alexa TrafficHistory action as JSON.
    """
    msg = 'Re-load of raw TrafficHistory Alexa JSON into Kafka error'
    assert parse_raw_traffichistory == (20123, 100, 100), msg


def test_flatten_single_traffichistory_xml():
    """Flatten a single TrafficHistory XML response from Alexa AWIS.
    """
    # Given an Alexa AWIS TrafficHistory XML response
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'single_traffichistory_result.xml')) as _fh:
        source_xml = _fh.read().rstrip()

    # when I flatten the XML
    awis = domain_intel.awis.actions.TrafficHistory()
    received = json.loads(awis.flatten_xml(source_xml))

    # then I should receive a list of JSON-encode domains
    msg = 'Flattened domain should produce JSON-encoded domain list'
    expected = '2017-06-01'
    assert received['TrafficHistory']['Start']['$'] == expected, msg


def test_flatten_empty_traffichistory_xml():
    """Flatten an empty TrafficHistory XML response from Alexa AWIS.
    """
    # Given an Alexa AWIS TrafficHistory XML response
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'single_traffichistory_empty.xml')) as _fh:
        source_xml = _fh.read().rstrip()

    # when I flatten the XML
    awis = domain_intel.awis.actions.TrafficHistory()
    received = json.loads(awis.flatten_xml(source_xml))

    # then I should receive a list of JSON-encode domains
    msg = 'Flattened domain should produce JSON-encoded domain list'
    expected = '2017-07-01'
    assert received['TrafficHistory']['Start']['$'] == expected, msg


def test_flatten_errored_traffichistory_xml():
    """Flatten an errored TrafficHistory XML response from Alexa AWIS.
    """
    # Given an Alexa AWIS TrafficHistory XML response
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'single_traffichistory_alexa_error.xml')) as _fh:
        source_xml = _fh.read().rstrip()

    # when I flatten the XML
    awis = domain_intel.awis.actions.TrafficHistory()
    received = awis.flatten_xml(source_xml)

    # then I should receive a list of JSON-encode domains
    msg = 'Flattened domain should produce JSON-encoded domain list'
    assert received is None, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_traffichistory')
def test_flatten_traffic(flatten_traffic):
    """Flatten Alexa TrafficHistory XML and publish.
    """
    msg = 'Flattening TrafficHistory Alexa XML back into Kafka error'
    assert flatten_traffic == (100, 100), msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'parse_raw_traffichistory',
                         'flatten_traffic')
def test_persist(persist_traffichistory):
    """Persist TrafficHistory to the database.
    """
    # ... then I should receive a record count if domains stored
    msg = 'Count against the "traffic" collection incorrect'
    assert persist_traffichistory == (100, 100), msg
