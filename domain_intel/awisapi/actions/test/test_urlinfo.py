""":class:`domain_intel.awisapi.actions.UrlInfo` unit test cases.

"""
from datetime import datetime
import mock
from future.moves.urllib.error import HTTPError

import domain_intel.awisapi.actions as awis_api


def test_urlinfo_init():
    """Initialise a domain_intel.awisapi.actions.UrlInfo object.
    """
    # When I initialise an Alexa API UrlInfo object
    api = awis_api.UrlInfo(None, None)

    # I should get a domain_intel.awisapi.actions.UrlInfo instance
    msg = 'Object is not a domain_intel.awisapi.actions.UrlInfo instance'
    assert isinstance(api, awis_api.UrlInfo), msg


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
        'Action': 'UrlInfo',
        'Url': 'google.com',
        'ResponseGroup':
            ('RelatedLinks,Categories,Rank,'
             'RankByCountry,UsageStats,AdultContent,'
             'Speed,Language,OwnedDomains,LinksInCount,'
             'SiteData')
    }

    # when I build the URL
    mock_dt.datetime.utcnow.return_value = datetime(2017, 3, 23)
    mock_dt.datetime.strptime.side_effect = (
        lambda *args, **kw: datetime.strptime(*args, **kw)
    )
    api = awis_api.UrlInfo(access_key, secret_key)
    received = api.build_url(params)

    # then I expect
    expected = ('http://awis.amazonaws.com/?'
                'AWSAccessKeyId=AKIAJLLAHHxxxxxxxxxx&'
                'Action=UrlInfo&'
                'ResponseGroup=RelatedLinks%'
                '2CCategories%2C'
                'Rank%2C'
                'RankByCountry%2C'
                'UsageStats%2C'
                'AdultContent%2C'
                'Speed%2C'
                'Language%2C'
                'OwnedDomains%2C'
                'LinksInCount%2C'
                'SiteData&'
                'Signature=2SF51zGJqd9yLq1rIW805WzWlKc%3D&'
                'SignatureMethod=HmacSHA1&'
                'SignatureVersion=2&'
                'Timestamp=2017-03-23T00%3A00%3A00.000Z&'
                'Url=google.com')
    msg = 'AWIS UrlInfo HTTP request URL build error'
    assert received == expected, msg


def test_build_domain_query_single_domain():
    """Build the AWIS domain HTTP query request component: single domain.
    """
    # Given a domain to search
    domain = 'google.com'

    # and a list of response groups
    response_groups = ['RelatedLinks', 'Categories']

    # when I build the domain HTTP request
    received = awis_api.UrlInfo.build_domain_query(domain, response_groups)

    # then I expect
    expected = {
        'ResponseGroup': 'RelatedLinks,Categories',
        'Url': 'google.com'
    }
    msg = 'Single domain search HTTP query build error'
    assert received == expected, msg


def test_build_domain_query_list_of_domains():
    """Build the AWIS domain HTTP query request component: list of domains.
    """
    # Given an AWS access key
    access_key = 'AKIAJLLAHHxxxxxxxxxx'

    # and secret
    secret_key = 'UiRBL2Tn/QKdiOxxxxxxxxxxxxxxxxxxxxxxxxxx'

    # and a list of domains to search
    domains = ['google.com', 'google.com.au']

    # and a list of response groups
    response_groups = ['RelatedLinks', 'Categories']

    # when I build the domain HTTP request
    api = awis_api.UrlInfo(access_key, secret_key)
    received = api.build_domain_query(domains, response_groups)

    # then I expect
    expected = {
        'UrlInfo.1.Url': 'google.com',
        'UrlInfo.2.Url': 'google.com.au',
        'UrlInfo.Shared.ResponseGroup': 'RelatedLinks,Categories'
    }
    msg = 'Multi-domain search HTTP query build error'
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
        'Action': 'UrlInfo',
        'Url': 'google.com',
        'ResponseGroup':
            ('RelatedLinks,Categories,Rank,'
             'RankByCountry,UsageStats,AdultContent,'
             'Speed,Language,OwnedDomains,LinksInCount,'
             'SiteData')
    }

    # when I build the URL
    mock_dt.datetime.utcnow.return_value = datetime(2017, 3, 23)
    mock_dt.datetime.strptime.side_effect = (
        lambda *args, **kw: datetime.strptime(*args, **kw)
    )
    api = awis_api.UrlInfo(access_key, secret_key)
    received = api.build_url(params)

    # then I expect
    expected = ('http://awis.amazonaws.com/?'
                'AWSAccessKeyId=AKIAJLLAHHxxxxxxxxxx&'
                'Action=UrlInfo&'
                'ResponseGroup=RelatedLinks%'
                '2CCategories%2C'
                'Rank%2C'
                'RankByCountry%2C'
                'UsageStats%2C'
                'AdultContent%2C'
                'Speed%2C'
                'Language%2C'
                'OwnedDomains%2C'
                'LinksInCount%2C'
                'SiteData&'
                'Signature=2SF51zGJqd9yLq1rIW805WzWlKc%3D&'
                'SignatureMethod=HmacSHA1&'
                'SignatureVersion=2&'
                'Timestamp=2017-03-23T00%3A00%3A00.000Z&'
                'Url=google.com')
    msg = 'AWIS UrlInfo HTTP request URL build error'
    assert received == expected, msg
