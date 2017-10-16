""":class:`domain_intel.awisapi.actions.TrafficHistory` unit test cases.

"""
from datetime import datetime, date, timedelta
import mock

import domain_intel.awisapi.actions as awis_api


def test_traffichistory_init():
    """Initialise a domain_intel.awisapi.actions.TrafficHistory object.
    """
    # When I initialise an Alexa API TrafficHistory object
    api = awis_api.TrafficHistory(None, None)

    # I should get a domain_intel.awisapi.actions.TrafficHistory instance
    msg = ('Object is not a domain_intel.awisapi.actions.TrafficHistory '
           'instance')
    assert isinstance(api, awis_api.TrafficHistory), msg


@mock.patch('domain_intel.awisapi.actions.traffichistory.datetime')
def test_build_query_last_month(mock_dt):
    """Build the AWIS domain HTTP query request component.
    """
    # Given a domain to search
    domain = 'google.com'

    # when I build the domain HTTP request
    mock_dt.date.today.return_value = date(2017, 3, 23)
    mock_dt.datetime.strftime.side_effect = (
        lambda *args, **kw: datetime.strftime(*args, **kw)
    )
    mock_dt.timedelta.side_effect = (
        lambda *args, **kw: timedelta(*args, **kw)
    )
    received = awis_api.TrafficHistory.build_query(domain)

    # then I expect
    expected = {
        'Range': 28,
        'ResponseGroup': 'History',
        'Start': '20170201',
        'Url': 'google.com'
    }
    msg = 'TrafficHistory domain search (last month) HTTP query build error'
    assert received == expected, msg


@mock.patch('domain_intel.awisapi.actions.traffichistory.datetime')
def test_build_query_2_month_back(mock_dt):
    """Build the AWIS domain HTTP query request component: 2 months back.
    """
    # Given a domain to search
    domain = 'google.com'

    # and a month range value beyond last month
    month_range = 1

    # when I build the domain HTTP request
    mock_dt.date.today.return_value = date(2017, 3, 23)
    mock_dt.datetime.strftime.side_effect = (
        lambda *args, **kw: datetime.strftime(*args, **kw)
    )
    mock_dt.timedelta.side_effect = (
        lambda *args, **kw: timedelta(*args, **kw)
    )
    received = awis_api.TrafficHistory.build_query(domain, month_range)

    # then I expect
    expected = {
        'Range': 31,
        'ResponseGroup': 'History',
        'Start': '20170101',
        'Url': 'google.com'
    }
    msg = 'TrafficHistory domain search (2 month back) HTTP query build error'
    assert received == expected, msg


@mock.patch('domain_intel.awisapi.actions.traffichistory.datetime')
def test_build_query_6_month_back(mock_dt):
    """Build the AWIS domain HTTP query request component: 6 months back.
    """
    # Given a domain to search
    domain = 'google.com'

    # and a month range value beyond last month
    month_range = 6

    # when I build the domain HTTP request
    mock_dt.date.today.return_value = date(2017, 3, 23)
    mock_dt.datetime.strftime.side_effect = (
        lambda *args, **kw: datetime.strftime(*args, **kw)
    )
    mock_dt.timedelta.side_effect = (
        lambda *args, **kw: timedelta(*args, **kw)
    )
    received = awis_api.TrafficHistory.build_query(domain, month_range)

    # then I expect
    expected = {
        'Range': 31,
        'ResponseGroup': 'History',
        'Start': '20160801',
        'Url': 'google.com'
    }
    msg = 'TrafficHistory domain search (2 month back) HTTP query build error'
    assert received == expected, msg


@mock.patch('domain_intel.awisapi.datetime')
def test_build_url(mock_dt):
    """Send HTTP request to AWIS.
    """
    # Given an AWS access key
    access_key = 'AKIAJLLAHHxxxxxxxxxx'

    # and secret
    secret_key = 'UiRBL2Tn/QKdiOxxxxxxxxxxxxxxxxxxxxxxxxxx'

    # and a set of query parameters
    params = {
        'Action': 'TrafficHistory',
        'Url': 'google.com',
        'ResponseGroup': 'History',
        'Range': 31,
        'Start': '20170201',
    }

    # when I build the URL
    mock_dt.datetime.utcnow.return_value = datetime(2017, 3, 23)
    mock_dt.datetime.strptime.side_effect = (
        lambda *args, **kw: datetime.strptime(*args, **kw)
    )
    api = awis_api.TrafficHistory(access_key, secret_key)
    received = api.build_url(params)

    # then I expect
    expected = ('http://awis.amazonaws.com/?'
                'AWSAccessKeyId=AKIAJLLAHHxxxxxxxxxx&'
                'Action=TrafficHistory&'
                'Range=31&'
                'ResponseGroup=History&'
                'Signature=LdgpdGabnCulqzAE2zqX9FMNllY%3D&'
                'SignatureMethod=HmacSHA1&'
                'SignatureVersion=2&'
                'Start=20170201&'
                'Timestamp=2017-03-23T00%3A00%3A00.000Z&'
                'Url=google.com')
    msg = 'AWIS TrafficHistory HTTP request URL build error'
    assert received == expected, msg
