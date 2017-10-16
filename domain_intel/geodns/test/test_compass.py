import pytest
import mock
import requests
from domain_intel import geodns

@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def test_compass_resolve_success(mock_post):
    """Successful request to compass service"""
    mock_post.return_value.status_code = 200
    mock_post.return_value.content = b"{}"

    ip = "203.122.134.12"
    time = 1494555429

    compass = geodns.CompassHTTPResolver(username="", password="")
    raw_compass_res = compass.resolve(ipv4=ip, time_epoch=time)

    assert raw_compass_res is not None, "compass.resolve(ip,time) with no exception should return data"

@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def test_compass_resolve_failure(mock_post):
    """Failing request to compass service"""

    mock_response = mock.Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
    mock_post.return_value = mock_response

    ip = "203.122.134.12"
    time = 1494555429

    compass = geodns.CompassHTTPResolver(username="", password="")

    try:
        raw_compass_res = compass.resolve(ipv4=ip, time_epoch=time)
    except Exception as exc:
        assert isinstance(exc, geodns.CompassServerError), "failing request should raise geodns.CompassServerError"
    else:
        assert False, "status_code 500 should have raised exception"


@mock.patch('domain_intel.geodns.compass.requests.sessions.Session.post')
def test_compass_resolve_empty(mock_post):
    """Empty result from compass, not a failure perse"""

    mock_post.return_value.content = b'{"Error":"no routes"}\n'
    mock_post.return_value.status_code = 500

    ip = "203.122.134.12"
    time = 1494555429

    compass = geodns.CompassHTTPResolver(username="", password="")

    try:
        compass.resolve(ipv4=ip, time_epoch=time)
    except Exception as exc:
        assert isinstance(exc, geodns.CompassServerEmptyResponse), "failing request should raise geodns.CompassServerEmptyResponse"
    else:
        assert False, "status_code 500 should have raised exception"
