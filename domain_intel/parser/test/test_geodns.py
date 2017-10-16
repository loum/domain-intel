""":class:`domain_intel.parser.GeoDNS` unit test cases.

"""
import os
import io
import json

import domain_intel.parser

SAMPLE_FILE_DIR = os.path.join('domain_intel',
                               'test',
                               'files',
                               'samples',
                               'geodns_flattened')

def test_parser_geodns_init():
    """Initialise a domain_intel.parser.GeoDNS object.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'avtt9.info'),
                 encoding='utf-8') as _fh:
        data = _fh.read().rstrip()

    # When I initialise an GeoDNS parser object
    awis = domain_intel.parser.GeoDNS(data)

    # I should get a domain_intel.parser.GeoDNS instance
    msg = 'Object is not a domain_intel.parser.GeoDNS instance'
    assert isinstance(awis, domain_intel.parser.GeoDNS), msg


def test_get_domain():
    """Extract the domain name from the GeoDNS data construct.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'avtt9.info'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read())

    # when I extract the domain name
    received = domain_intel.parser.GeoDNS._get_domain(data)

    # then I should match the domain name
    msg = 'Extracted domain name not "avtt9.info"'
    assert received == 'avtt9.info', msg 


def test_db_ipv4_vertex():
    """Extract the IPv4 content from the GeoDNS construct.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'avtt9.info'),
                 encoding='utf-8') as _fh:
        data = _fh.read()

    # when I extract the IPv4 content
    parser = domain_intel.parser.GeoDNS(data)
    received = parser.db_ipv4_vertex

    # then I should match the IPv4 list
    msg = 'Extracted IPv4 content error'
    expected = {
        '_key': '162.210.196.168',
        'city': {'name': 'Manassas', 'timezone_db': 'America/New_York'},
        'connection_speed': {'name': 'Corporate'},
        'continent': {'code': 'NA', 'name': 'North America'},
        'country': {'iso3166_code_2': 'US',
                    'iso3166_code_3': '',
                    'name': 'United States'},
        'geospatial': {'accuracy_radius': 50,
                       'latitude': 38.7701,
                       'longitude': -77.6321,
                       'postcode_end': '20109',
                       'postcode_start': '20109'},
        'id': 8427276,
        'ip': '162.210.196.168',
        'isp': {'name': 'Leaseweb USA'},
        'organisation': {'name': 'Leaseweb USA'},
        'region': {'iso3166_code_2': 'VA', 'name': 'Virginia'},
        'time': 1496877359
    }
    assert received[2] == expected, msg 


def test_db_ipv6_vertex():
    """Extract the IPv6 content from the GeoDNS construct.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'themelock.com'),
                 encoding='utf-8') as _fh:
        data = _fh.read()

    # when I extract the IPv6 content
    parser = domain_intel.parser.GeoDNS(data)
    received = parser.db_ipv6_vertex

    # then I should match the IPv6 list
    msg = 'Extracted IPv6 content error'
    expected = [
        {'_key': '2400:cb00:2048:1::681c:226'},
        {'_key': '2400:cb00:2048:1::681c:326'},
    ]
    assert received == expected, msg 


def test_db_ipv4_edge():
    """Extract the IPv4 content from the GeoDNS construct for edge insert.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'avtt9.info'),
                 encoding='utf-8') as _fh:
        data = _fh.read()

    # when I extract the IPv4 edge content
    parser = domain_intel.parser.GeoDNS(data)
    received = parser.db_ipv4_edge

    # then I should match the IPv4 list
    msg = 'Extracted IPv4 edge structure error'
    expected = [
        {
            '_from': 'domain/avtt9.info',
            '_key': 'avtt9.info:149.202.120.43',
            '_to': 'ipv4/149.202.120.43'
        },
        {
            '_from': 'domain/avtt9.info',
            '_key': 'avtt9.info:158.69.143.109',
            '_to': 'ipv4/158.69.143.109'
        },
        {
            '_from': 'domain/avtt9.info',
            '_key': 'avtt9.info:162.210.196.168',
            '_to': 'ipv4/162.210.196.168'
        },
        {
            '_from': 'domain/avtt9.info',
            '_key': 'avtt9.info:37.48.65.136',
            '_to': 'ipv4/37.48.65.136'
        },
        {
            '_from': 'domain/avtt9.info',
            '_key': 'avtt9.info:37.48.65.155',
            '_to': 'ipv4/37.48.65.155'
        }
    ]
    assert received == expected, msg 


def test_db_ipv6_edge():
    """Extract the IPv6 content from the GeoDNS construct for edge insert.
    """
    # Given a flattend GeoDNS construct
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'avtt9.info'),
                 encoding='utf-8') as _fh:
        data = _fh.read()

    # when I extract the IPv6 edge content
    parser = domain_intel.parser.GeoDNS(data)
    received = parser.db_ipv6_edge

    # then I should match the IPv6 list
    msg = 'Extracted IPv6 edge structure error'
    expected = []
    assert received == expected, msg 
