""":class:`domain_intel.parser.TrafficHistory` unit test cases.

"""
import os
import io
import json

import domain_intel.parser

SAMPLE_FILE_DIR = os.path.join('domain_intel',
                               'test',
                               'files',
                               'samples',
                               'traffic_flattened')


def test_parser_traffichistory_init():
    """Initialise a domain_intel.parser.TrafficHistory object.
    """
    # Given a flattend TrafficHistory construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'ondertitel.com'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # When I initialise an TrafficHistory parser object
    awis = domain_intel.parser.TrafficHistory(data)

    # I should get a domain_intel.parser.TrafficHistory instance
    msg = 'Object is not a domain_intel.parser.TrafficHistory instance'
    assert isinstance(awis, domain_intel.parser.TrafficHistory), msg


def test_get_domain():
    """Extract the domain name from the TrafficHistory data construct.
    """
    # Given a flattend TrafficHistory construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'ondertitel.com'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # when I extract the domain name
    received = domain_intel.parser.TrafficHistory._get_domain(data)

    # then I should match the domain name
    msg = 'Extracted domain name not "ondertitel.com"'
    assert received == 'ondertitel.com', msg
