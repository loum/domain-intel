""":class:`domain_intel.awis.actions.SitesLinkingIn` unit test cases.

"""
import os
import json
import pytest
import mock

import domain_intel.awis.actions
import domain_intel.utils


def test_awis_siteslinkingin_init():
    """Initialise a domain_intel.awis.actions.SitesLinkingIn object.
    """
    # When I initialise an Alexa Web Information object
    awis = domain_intel.awis.actions.SitesLinkingIn()

    # I should get a domain_intel.awis.actions.SitesLinkingIn instance
    msg = 'Object not a domain_intel.awis.actions.SitesLinkingIn instance'
    assert isinstance(awis, domain_intel.awis.actions.SitesLinkingIn), msg


@mock.patch('domain_intel.awis.actions.SitesLinkingIn.api')
def test_slurp_sites_linking_in(mock_api):
    """Get list of sites linking into a given domain.
    """
    # Given a domain name
    domain = 'ondertitel.com'

    # when I slurp for a list of domains that link into that domain
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'batch_alexa_siteslinkingin.json')) as _fh:
        json_results = json.loads(_fh.read().rstrip())
        json_results = [bytearray(x, 'utf-8') for x in json_results]
        mock_api.sites_linking_in.side_effect = json_results
    awis = domain_intel.awis.actions.SitesLinkingIn()
    received = awis.slurp_sites_linking_in(domain=domain)

    # then I should get a list of sites
    msg = 'List of sites linking in error'
    assert len(received) == 224, msg


def test_unique_titles():
    """Remove duplicate titles from the SitesLinkingIn results set.
    """
    # Given a list of raw SitesLinkingIn titles
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'siteslinkingin_titles.json')) as _fh:
        titles = json.loads(_fh.read().rstrip())

    # when I unique the list
    received = domain_intel.awis.actions.SitesLinkingIn.unique_titles(titles)
    # then I should get a reduced list of titles
    msg = 'Uniqued SitesLinkingIn list not reduced'
    assert len(received) == 224, msg


@mock.patch('domain_intel.awis.actions.SitesLinkingIn.api')
def test_slurp_sites_linking_in_with_threshold(mock_api):
    """Get list of sites linking into a given domain.
    """
    # Given a domain name
    domain = 'ondertitel.com'

    # and an upstream slurp count threshold
    threshold = 3

    # when I slurp for a list of domains that link into that domain
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'batch_alexa_siteslinkingin.json')) as _fh:
        json_results = json.loads(_fh.read().rstrip())
        json_results = [bytearray(x, 'utf-8') for x in json_results]
        mock_api.sites_linking_in.side_effect = json_results
    awis = domain_intel.awis.actions.SitesLinkingIn()
    received = awis.slurp_sites_linking_in(domain=domain,
                                           max_slurps=threshold)

    # then I should get a list of sites
    msg = 'List of sites linking in error'
    assert len(received) == 60, msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_add_domains(add_siteslinkingin_domains):
    """Load of SitesLinkingIn domains into Kafka topic.
    """
    msg = 'Load of SitesLinkingIn domains into Kafka error'
    assert add_siteslinkingin_domains == 100, msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_parse_raw_siteslinkingin(parse_raw_siteslinkingin):
    """Parse raw Alexa SitesLinkingIn action as JSON.
    """
    msg = 'Re-load of raw SitesLinkingIn Alexa JSON into Kafka error'
    assert parse_raw_siteslinkingin == (68, 68), msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'parse_raw_siteslinkingin')
def test_persist(persist_siteslinkingin):
    """Persist SitesLinkingIn to the database.
    """
    # ... then I should receive a record count if domains stored
    msg = 'Count against the domain collection incorrect'
    assert persist_siteslinkingin == (68, 786), msg
