""":class:`domain_intel.awisapi.AwisApi` unit test cases.

"""
from datetime import datetime
import mock
from future.moves.urllib.error import HTTPError

import domain_intel.awisapi


def test_awisapi_init():
    """Initialise a domain_intel.awisapi.AwisApi object.
    """
    # When I initialise an Awis API object
    api = domain_intel.awisapi.AwisApi(None, None)

    # I should get a domain_intel.awisapi.AwisApi instance
    msg = 'Object is not a domain_intel.AwisApi instance'
    assert isinstance(api, domain_intel.awisapi.AwisApi), msg


def test_signature():
    """Calculate the AWS Signature v2.
    """
    # Given an AWS access key
    access_key = 'AKIAJLLAHHxxxxxxxxxx'

    # and secret
    secret_key = 'UiRBL2Tn/QKdiOxxxxxxxxxxxxxxxxxxxxxxxxxx'

    # and a parameter list
    params = {
        'Action': 'UrlInfo',
        'Url': 'google.com',
        'ResponseGroup': 'Rank,LinksInCount',
        'AWSAccessKeyId': access_key,
        'SignatureMethod': 'HmacSHA1',
        'SignatureVersion': 2,
        'Timestamp': '2017-04-27T03:01:30.000Z'
    }

    # when I calculate the signature
    api = domain_intel.awisapi.AwisApi(access_key, secret_key)
    received = api.calculate_signature(params)

    # then I expect
    msg = 'Error calculating signature'
    assert received == b'vJgaj9iiyiFs7aeG9AhPcNJlkSQ=', msg


@mock.patch('domain_intel.awisapi.urlopen')
def test_request_success(mock_urlopen):
    """Request to AWIS.
    """
    # Given an AWS access key
    access_key = 'AKIAJLLAHHxxxxxxxxxx'

    # and secret
    secret_key = 'UiRBL2Tn/QKdiOxxxxxxxxxxxxxxxxxxxxxxxxxx'

    # and an AWIS URL
    url = ('http://awis.amazonaws.com/?'
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

    # when I execute a request
    mock_urlopen.return_value.code = 200
    mock_urlopen.return_value.read.return_value = 'OK'
    api = domain_intel.awisapi.AwisApi(access_key, secret_key)
    received = api.request(url)

    # then I should not receive None
    msg = 'Valid HTTP query response should not be None'
    assert received is not None, msg


@mock.patch('domain_intel.awisapi.urlopen')
def test_request_exception(mock_urlopen):
    """Request to AWIS.
    """
    # Given an AWS access key
    access_key = 'AKIAJLLAHHxxxxxxxxxx'

    # and secret
    secret_key = 'UiRBL2Tn/QKdiOxxxxxxxxxxxxxxxxxxxxxxxxxx'

    # and an AWIS URL
    url = ('http://awis.amazonaws.com/?'
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

    # when I execute a request
    mock_urlopen.side_effect = HTTPError(url,
                                         503,
                                         'Service Unavailable',
                                         {},
                                         None)
    api = domain_intel.awisapi.AwisApi(access_key, secret_key)
    received = api.request(url)

    # then I should not receive None
    msg = 'Valid HTTP query response should not be None'
    assert received is None, msg
