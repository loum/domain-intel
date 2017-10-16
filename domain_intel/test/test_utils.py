""":module:`domain_intel.utils` unit test cases.

"""
import os
import json
import time
import mock
import pytest

import domain_intel.utils
import domain_intel.common

CONFIG = domain_intel.common.CONFIG


@pytest.mark.usefixtures('docker_compose')
def test_info():
    """Dump Kafka topic information.
    """
    # When I dump information around the topics I can access
    kwargs = {'bootstrap_servers': 'localhost:9091'}
    received = domain_intel.utils.info(**kwargs)

    # then I should receive a list of topics that I can access
    msg = 'List of accessible Kafka topics error'
    expected = [x.split(':')[0] for x in CONFIG.get('kafka')['topics'].split(',')]
    assert sorted(received) == sorted(expected), msg


def test_epoch_from_str():
    """Convert timestamp to epoch.
    """
    # Given a date string
    date = '2017-06-01'

    # and a date format
    date_format = '%Y-%m-%d'

    # when I convert to epoch time
    received = domain_intel.utils.epoch_from_str(date, date_format)

    # then I should get an expected result
    expected = 1496275200
    msg = 'Epoch time conversion error'
    assert received == expected, msg


def test_epoch_from_str_invalid_format():
    """Convert timestamp to epoch: invalid format.
    """
    # Given a date string
    date = '2017-06-01'

    # and an invalid date format
    date_format = '%y'

    # when I convert to epoch time
    received = domain_intel.utils.epoch_from_str(date, date_format)

    # then I should received None
    msg = 'Epoch time conversion with invalid date format should be None'
    assert received is None, msg


def test_analyst_xls_to_json():
    """Convert Excel to JSON.
    """
    # Given a source Excel file
    xls_file = os.path.join('domain_intel',
                            'test',
                            'files',
                            'samples',
                            'dis_analyst_top_200.xls')

    # when I convert to JSON
    received = list(domain_intel.utils.analyst_xls_to_json(xls_file,
                                                           dry=True))


    # then I should receive a dictionary structure
    msg = 'XLS to JSON conversion error'
    expected = {
        'domain_down_or_parked': 'N',
        'has_forum_or_comments': 'N',
        'has_rss_feed': 'N',
        'links_to_osp': 'Y',
        'links_to_torrents': 'Y',
        'p2p_magnet_links': 'N',
        'requires_login': 'N',
        'search_feature': 'Y'
    }
    assert json.loads(received[0]).get('4shared.com') == expected, msg


@mock.patch('domain_intel.utils.time')
def test_get_epoch_ranges(mock_time):
    """Define epoch time ranges.
    """
    # Given a point in time
    mock_time.gmtime.return_value = time.gmtime(1501820618)

    # when I determine the epoch ranges for last month
    received = domain_intel.utils.get_epoch_ranges()

    # then I should receive a tuple of epoch times
    msg = 'Epoch time ranges for last month incorrect'
    expected = (1498867200.0, 1501459200.0)
    assert received == expected, msg


@mock.patch('domain_intel.utils.time')
def test_get_epoch_ranges_3_month_range(mock_time):
    """Define epoch time ranges: 3 month range.
    """
    # Given a point in time
    mock_time.gmtime.return_value = time.gmtime(1501820618)

    # and a month range
    months = 2

    # when I determine the epoch ranges for last month
    received = domain_intel.utils.get_epoch_ranges(months=months)

    # then I should receive a tuple of epoch times
    msg = 'Epoch time ranges for last month incorrect'
    expected = (1493596800.0, 1501459200.0)
    assert received == expected, msg
