import pytest
import mock
import requests
import re
import os

from domain_intel import geodns

geodns_sample_dir = os.path.join(
    'domain_intel',
    'geodns',
    'test',
    'files',
    'samples',
    'geodns',
)

_check_dns_res = open(os.path.join(geodns_sample_dir, 'check_dns.out')).read()
_check_result_res = open(os.path.join(geodns_sample_dir, 'check_result.out')).read()

@mock.patch('domain_intel.geodns.checkhostnet.requests.sessions.Session.get')
def test_resolve_dns_fullstructure(mock_session):

    # case on the url coming through get, return responses to the right urls
    # not very happy with this, but i don't want to introduce useless wrapper
    # methods like '_get_check_result' just to test it.
    def _smart_get(url, *args, **kwargs):
        res = mock.MagicMock()
        res.status_code = 200
        if re.search("/check-dns\?", url):
            res.content = _check_dns_res.encode("utf-8")
        elif re.search("/check-result/", url):
            res.content = _check_result_res.encode("utf-8")
        else:
            raise ValueError("mocked get'er only handles check-geodns and check-result endpoints, got %s" % url)
        return res

    mock_session.side_effect = _smart_get

    domain = "www.ip-echelon.com"
    api = geodns.CheckHostNet()
    response = api.resolve_dns(domain=domain)

    # response should be correct container type
    assert isinstance(response, geodns.CheckHostNetResult), "resolve_dns(domain) should return CheckHostNetResult"
    # response should be serialisable with pickle
    assert len(response.marshal()) > 0, "resolve_dns(domain) should return serialisable result of len > 0"

    # response should roundtrip through serialisation to the same (looking) object
    roundtripped = geodns.CheckHostNetResult.from_serialised(response.marshal())
    assert roundtripped == response, "serialisation roundtripped response should be identical"


@mock.patch('domain_intel.geodns.checkhostnet.requests.sessions.Session.get')
def test_get_or_error_success(mock_get):
    """Successful request to check-host.net"""
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"{}"

    url = "doesnt-matter.com"
    domain = "ip-echelon.com"

    api = geodns.CheckHostNet()

    raw_api_res = api._get_or_error(
        url,
        headers=api.headers,
    )

    assert raw_api_res is not None, "_get_or_error with no exception should return data"

@mock.patch('domain_intel.geodns.checkhostnet.requests.sessions.Session.get')
def test_get_or_error_failure(mock_get):
    """Failing request to check-host.net"""

    mock_response = mock.Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
    mock_get.return_value = mock_response

    url = "doesnt-matter.com"
    domain = "ip-echelon.com"

    api = geodns.CheckHostNet()

    try:
        raw_api_res = api._get_or_error(
            url,
            headers=api.headers,
        )
    except Exception as exc:
        assert isinstance(exc, geodns.CheckHostNetError), "failing request should raise geodns.checkHostNetError"
    else:
        assert False, "status_code 500 should have raised exception"
