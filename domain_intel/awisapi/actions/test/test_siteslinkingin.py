""":class:`domain_intel.awisapi.actions.SitesLinkingIn` unit test cases.

"""
from datetime import datetime
import mock

import domain_intel.awisapi.actions as awis_api


def test_siteslinkingin_init():
    """Initialise a domain_intel.awisapi.actions.SitesLinkingIn object.
    """
    # When I initialise an Alexa API SitesLinkingIn object
    api = awis_api.SitesLinkingIn(None, None)

    # I should get a domain_intel.awisapi.actions.SitesLinkingIn instance
    msg = ('Object is not a domain_intel.awisapi.actions.SitesLinkingIn '
           'instance')
    assert isinstance(api, awis_api.SitesLinkingIn), msg


def test_build_query():
    """Build the AWIS domain HTTP query request component.
    """
    # Given a domain to search
    domain = 'google.com'

    # when I build the domain HTTP request
    received = awis_api.SitesLinkingIn.build_query(domain)

    # then I expect
    expected = {
        'Count': 20,
        'ResponseGroup': 'SitesLinkingIn',
        'Start': 0,
        'Url': 'google.com'
    }
    msg = 'SitesLinkingIn domain search HTTP query build error'
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
        'Action': 'SitesLinkingIn',
        'Url': 'google.com',
        'ResponseGroup': 'SitesLinkingIn',
        'Count': 20,
        'Start': 0,
    }

    # when I build the URL
    mock_dt.datetime.utcnow.return_value = datetime(2017, 3, 23)
    mock_dt.datetime.strptime.side_effect = (
        lambda *args, **kw: datetime.strptime(*args, **kw)
    )
    api = awis_api.SitesLinkingIn(access_key, secret_key)
    received = api.build_url(params)

    # then I expect
    expected = ('http://awis.amazonaws.com/?'
                'AWSAccessKeyId=AKIAJLLAHHxxxxxxxxxx&'
                'Action=SitesLinkingIn&'
                'Count=20&'
                'ResponseGroup=SitesLinkingIn&'
                'Signature=Pl3FHsSGuXvAZ6Oq3UcEHC0CW4Y%3D&'
                'SignatureMethod=HmacSHA1&'
                'SignatureVersion=2&'
                'Start=0&'
                'Timestamp=2017-03-23T00%3A00%3A00.000Z&'
                'Url=google.com')
    msg = 'AWIS SitesLinkinIn HTTP request URL build error'
    assert received == expected, msg
