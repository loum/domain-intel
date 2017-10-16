""":class:`domain_intel.analyst.Qas` unit test cases.

"""
import pytest

import domain_intel.analyst


def test_analyst_qas_init():
    """Initialise a domain_intel.analyst.Qas object.
    """
    # When I initialise an Analyst Question and Answer object
    awis = domain_intel.analyst.Qas()

    # I should get a domain_intel.analyst.Qas instance
    msg = 'Object not a domain_intel.analyst.Qas instance'
    assert isinstance(awis, domain_intel.analyst.Qas), msg


@pytest.mark.usefixtures('docker_compose', 'kafka_ready')
def test_parse_qas(parse_qas):
    """Add raw Analyst QAs to Kafka.
    """
    msg = 'Load of Analyst QAs into Kafka error'
    assert parse_qas == 200, msg


@pytest.mark.usefixtures('docker_compose',
                         'kafka_ready',
                         'parse_raw_urlinfo',
                         'flatten_domains',
                         'init_arango',
                         'persist_domains',
                         'parse_qas')
def test_persist(persist_analystqas):
    """Persist Analyst QAs to the database.
    """
    # ... then I should receive a record count if QAs stored
    msg = 'Count against the "analyst-qas" collection incorrect'
    assert persist_analystqas == (200, 200), msg
