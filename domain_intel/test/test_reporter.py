""":class:`domain_intel.Reporter` unit test cases.

"""
import os
import time
import io
import json
import mock

import domain_intel

SAMPLE_FILE_DIR = os.path.join('domain_intel',
                               'test',
                               'files',
                               'samples')


def test_reporter():
    """Initialise a domain_intel.Reporter object.
    """
    # When I initialise an Domain Intel Reporter
    awis = domain_intel.Reporter(data=None)

    # I should get a domain_intel.Reporter instance
    msg = 'Object is not a domain_intel.Reporter instance'
    assert isinstance(awis, domain_intel.Reporter), msg


def test_reporter_parse():
    """Parse a Domain Intel graph construct.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I parse the graph construct
    reporter = domain_intel.Reporter(data=record)

    # then the domain attribute should return a value
    msg = 'Reporter.domain error'
    expected = {
        'DOMAIN': 'ondertitel.com',
        'ADULT_CONTENT': False,
        'LINKS_IN_COUNT': 141,
        'ENCODING': 'iso-8859-1',
        'LOCALE': 'nl-NL',
        'MEDIAN_LOAD_TIME': 767,
        'DESCRIPTION':
            '"Database voor het uploaden en downloaden van Nederlandse '
            'ondertitels voor Divx/Xvid."',
        'ONLINE_SINCE': '23-Feb-2004',
        'RANK': 86580,
        'SPEED_PERCENTILE': 87,
        'TITLE': '"Ondertitel.com"',
    }
    assert reporter.domain == expected, msg

    # and the countries attribute should return a value
    msg = 'Reporter.countries error'
    expected = [
        'country/BE',
        'country/DE',
        'country/NL'
    ]
    assert sorted(reporter.countries) == sorted(expected), msg


def test_get_country_ranks():
    """Get country ranks.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I parse the country ranks
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_country_ranks()

    # then I should receive a list of rank structures
    msg = 'Country rank error'
    expected = [
        {'COUNTRY_CODE': 'BE', 'COUNTRY_NAME': 'Belgium', 'COUNTRY_RANK': 1440},
        {'COUNTRY_CODE': 'DE', 'COUNTRY_NAME': 'Germany', 'COUNTRY_RANK': 45635},
        {'COUNTRY_CODE': 'NL', 'COUNTRY_NAME': 'Netherlands', 'COUNTRY_RANK': 2500}
    ]
    assert received == expected, msg


def test_get_sites_linking_in():
    """Get sites linking in.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I parse the sites linking in
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_sites_linking_in()

    # then I should receive a list of URLs linking in
    msg = 'URL linking in error'
    expected = [
        {
            'DOMAIN_LINKINGIN': u'kaskus.co.id',
            'URL_LINKINGIN':
                u'"archive.kaskus.co.id:80/thread/13385296/1"'
        },
        {
            'DOMAIN_LINKINGIN': u'stuffgate.com',
            'URL_LINKINGIN': u'"stuffgate.com:80/stuff/website/top-113000-sites"'
        },
        {
            'DOMAIN_LINKINGIN': u'aljyyosh.com',
            'URL_LINKINGIN': u'"aljyyosh.com:80/vb/showthread.php?t=12598"'
        },
        {
            'DOMAIN_LINKINGIN': u'dmoztools.net',
            'URL_LINKINGIN':
                u'"dmoztools.net:80/World/Nederlands/Computers/Multimedia/Beeld_en_Video"'
        },
        {
            'DOMAIN_LINKINGIN': u'bsplayer.com',
            'URL_LINKINGIN':
                u'"forum.bsplayer.com:80/general-talk-support/6000-read-first-before-posting.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'skynet.be',
            'URL_LINKINGIN': u'"users.skynet.be:80/nedsites/film.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'infonu.nl',
            'URL_LINKINGIN':
                u'"pc-en-internet.infonu.nl:80/tutorials/'
                '31155-ondertiteling-onder-film-zetten.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'napiprojekt.pl',
            'URL_LINKINGIN':
                u'"forum.napiprojekt.pl:80/viewtopic.php?t=149"'
        },
        {
            'DOMAIN_LINKINGIN': u'subtitleseeker.com',
            'URL_LINKINGIN':
                u'"subtitleseeker.com:80/Download-movie-1000292/'
                'Its%20a%20Mad%20Mad%20Mad%20Mad%20World%201963-NTSC"'
        },
        {
            'DOMAIN_LINKINGIN': u'amara.org',
            'URL_LINKINGIN':
                u'"amara.org:80/en/videos/G5NFTlUp42ul/hai/1657138/4759909"'
        },
        {
            'DOMAIN_LINKINGIN': u'startpagina.nl',
            'URL_LINKINGIN':
                u'"startpagina.nl:80/v/overig/vraag/29532/lettertype-gebruiken-ondertiteling"'
        },
        {
            'DOMAIN_LINKINGIN': u'blogspot.ch',
            'URL_LINKINGIN':
                u'"acrossborders.blogspot.ch:80/2006/10/'
                'can-you-ever-say-nigger-without-making.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'findeen.com',
            'URL_LINKINGIN': u'"be.findeen.com:80/8_mile_watch_online_subs.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'telenet.be',
            'URL_LINKINGIN': u'"users.telenet.be:80/alenkin/archive.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'scoop.it',
            'URL_LINKINGIN':
                u'"scoop.it:80/t/confbosimptoude/p/4075409398/'
                '2017/02/17/razer-nostromo-software-download-chip?"'
        },
        {
            'DOMAIN_LINKINGIN': u'blogspot.cl',
            'URL_LINKINGIN': u'"streptococcuspyogenes.blogspot.cl:80/2006/06/introduccin_15.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'blogspot.pe',
            'URL_LINKINGIN': u'"streptococcuspyogenes.blogspot.pe:80/2006/06/introduccin_15.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'cocolog-nifty.com',
            'URL_LINKINGIN': u'"eurobeter-gc8.cocolog-nifty.com:80/blog/2006/07/post_5b50.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'blogspot.tw',
            'URL_LINKINGIN': u'"redmotion.blogspot.tw:80/2008/10/new-ice-compounds.html"'
        },
        {
            'DOMAIN_LINKINGIN': u'secureserver.net',
            'URL_LINKINGIN': u'"ip-173-201-142-193.ip.secureserver.net:80/alexa/Alexa_25.html"',
        }
    ]
    assert received == expected, msg


def test_get_geodns():
    """Get GeoDNS data.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I parse the GeoDNS
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_geodns()

    # then I should receive a list of GeoDNS records
    msg = 'GeoDNS error'
    expected = [
        {
            'IPV4_CONTINENT': u'North America',
            'IPV4_CONTINENT_CODE': u'NA',
            'IPV4_COUNTRY': u'United States',
            'IPV4_COUNTRY_CODE': u'US',
            'IPV4_LATITUDE': 37.7697,
            'IPV4_LONGITUDE': -122.3933,
            'IPV4_ADDR': u'104.27.140.239',
            'IPV4_ORG': u'"CloudFlare"',
            'IPV4_ISP': u'"CloudFlare"',
        },
        {
            'IPV4_CONTINENT': u'North America',
            'IPV4_CONTINENT_CODE': u'NA',
            'IPV4_COUNTRY': u'United States',
            'IPV4_COUNTRY_CODE': u'US',
            'IPV4_LATITUDE': 37.7697,
            'IPV4_LONGITUDE': -122.3933,
            'IPV4_ADDR': u'104.27.141.239',
            'IPV4_ORG': u'"CloudFlare"',
            'IPV4_ISP': u'"CloudFlare"',
        },
    ]
    assert received == expected, msg


def test_get_geodns_dumping_none_values():
    """Get GeoDNS data: dumping None values.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record_indiecade.com.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I parse the GeoDNS
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_geodns()

    # then I should receive a list of GeoDNS records
    msg = 'GeoDNS error'
    expected = [
        {
            'IPV4_ADDR': '216.157.102.147',
            'IPV4_CONTINENT': '',
            'IPV4_CONTINENT_CODE': '',
            'IPV4_COUNTRY': '',
            'IPV4_COUNTRY_CODE': '',
            'IPV4_ISP': '',
            'IPV4_LATITUDE': '',
            'IPV4_LONGITUDE': '',
            'IPV4_ORG': '',
        },
    ]
    assert received == expected, msg


def test_get_traffichistory():
    """Get TrafficHistory data.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_traffichistory()

    # then I should receive a list of TrafficHistory records
    msg = 'TrafficHistory error'
    expected = {
        'TRAFFIC_PAGE_VIEWS_PM': 0.27,
        'TRAFFIC_PAGE_VIEWS_USER': 4,
        'TRAFFIC_RANK': 202346,
        'TRAFFIC_REACH': 3,
        'TRAFFIC_TS': 1497139200.0
    }
    assert received[10] == expected, msg


def test_get_traffichistory_dodgy_domain_during_filter():
    """Get TrafficHistory data: dodgy domain during filtering.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              'graph_record_trafficestimate.com.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_traffichistory()

    # then I should receive a list of TrafficHistory records
    msg = 'TrafficHistory parse error'
    expected = {
        'TRAFFIC_PAGE_VIEWS_PM': 0.68,
        'TRAFFIC_PAGE_VIEWS_USER': 3.3,
        'TRAFFIC_RANK': 87993,
        'TRAFFIC_REACH': 8.3,
        'TRAFFIC_TS': 1498953600.0
    }
    assert received[1] == expected, msg


def test_get_traffichistory_multiple_traffic_results():
    """Get TrafficHistory data: multiple traffic results.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'majaa.net.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_traffichistory()

    # then I should receive a list of TrafficHistory records
    msg = 'TrafficHistory with 3 data sets error'
    assert len(received) == 12, msg


def test_parse_traffic_history():
    """Parse TrafficHistory data.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              'graph_record_traffichistory.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # when I parse the TrafficHistory component
    received = domain_intel.Reporter._parse_traffic_history(data)

    # then I should receive a list of TrafficHistory records
    msg = 'TrafficHistory error'
    expected = [
        [1496275200.0, 0.33, 2, 132065, 6],
        [1496361600.0, 0.32, 5, 212337, 3],
        [1496448000.0, 0.68, 4, 99932, 7],
        [1496534400.0, 0.66, 3, 88803, 8],
        [1496620800.0, 0.49, 4, 124613, 6],
        [1496707200.0, 0.34, 3, 155241, 5],
        [1496793600.0, 0.19, 3, 257491, 3],
        [1496880000.0, 0.1, 2, 288875, 3],
        [1496966400.0, 0.1, 4, 468334, 1],
        [1497052800.0, 0.2, 3, 290383, 2],
        [1497139200.0, 0.27, 4, 202346, 3],
        [1497225600.0, 0.28, 3, 159624, 5],
        [1497312000.0, 0.26, 3, 202998, 4],
        [1497398400.0, 0.1, 2, 282946, 3],
        [1497484800.0, 0.72, 5, 114368, 6],
        [1497571200.0, 0.27, 5, 235407, 3],
        [1497657600.0, 0.37, 3, 131291, 5],
        [1497744000.0, 0.2, 3, 219011, 3],
        [1497830400.0, 0.8, 6, 112664, 6],
        [1497916800.0, 0.29, 4, 212608, 3],
        [1498003200.0, '', '', 1554469, ''],
        [1498089600.0, 0.77, 8, 141458, 4],
        [1498176000.0, 0.08, 2, 357855, 2],
        [1498262400.0, 0.62, 4, 101244, 7],
        [1498348800.0, 0.49, 2, 99977, 8],
        [1498435200.0, 0.25, 7, 331050, 2],
        [1498521600.0, 0.22, 4, 249188, 3],
        [1498608000.0, 0.4, 3, 137428, 6],
        [1498694400.0, 0.35, 4, 163201, 4],
        [1498780800.0, 0.4, 4, 145430, 5],
    ]
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends(mock_time):
    """Get TrafficHistory trends for last month.
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data)

    # then I should receive a delta value
    msg = 'Down-trend delta response error'
    expected = 101.44
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_with_empty_values(mock_time):
    """Get TrafficHistory trends for last month.
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              'ondertitel.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1498867200)
    received = domain_intel.Reporter.get_traffic_trends(data)

    # then I should receive a delta value
    msg = 'Down-trend delta response error'
    expected = 0.48
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_last_3_months(mock_time):
    """Get TrafficHistory trends for last 3 months.
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a month range
    months = 2

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data, months=months)

    # then I should receive a delta value
    msg = 'Down-trend delta response error'
    expected = 101.44
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_over_rank(mock_time):
    """Get TrafficHistory trends (rank) for last month.
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a TrafficRank CSV key
    key = 'TRAFFIC_RANK'

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data, key=key)

    # then I should receive a delta value
    msg = 'Rank down-trend delta response error'
    expected = 1038.89
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_over_rank_last_3_months(mock_time):
    """Get TrafficHistory trends (rank) for last 3 months.
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a TrafficRank CSV key
    key = 'TRAFFIC_RANK'

    # and a month range
    months = 2

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data,
                                                        months=months,
                                                        key=key)

    # then I should receive a delta value
    msg = 'Down-trend delta response error'
    expected = 1038.89
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_uptrend(mock_time):
    """Get TrafficHistory trends for last month (uptrend).
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # when I extract the TrafficHistory data uptrend
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data,
                                                        downtrend=False)

    # then I should receive an uptrend delta value
    msg = 'Up-trend delta response error'
    expected = 285.24
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_last_3_months_uptrend(mock_time):
    """Get TrafficHistory trends for last 3 months (uptrend).
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a month range
    months = 2

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data,
                                                        months=months,
                                                        downtrend=False)

    # then I should receive an uptrend delta value
    msg = 'Down-trend delta response error'
    expected = 285.24
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_over_rank_uptrend(mock_time):
    """Get TrafficHistory trends (rank) for last month (uptrend).
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a TrafficRank CSV key
    key = 'TRAFFIC_RANK'

    # when I extract the TrafficHistory data
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data,
                                                        key=key,
                                                        downtrend=False)

    # then I should receive an uptrend delta value
    msg = 'Rank up-trend delta response error'
    expected = 117.82
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_get_traffic_trends_over_rank_last_3_months_uptrend(mock_time):
    """Get TrafficHistory trends (rank) for last 3 months (uptrend).
    """
    # Given a set of TrafficHistory data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              '4shared.com_traffic_history.json'),
                 encoding='utf-8') as _fh:
        data = json.loads(_fh.read().rstrip())

    # and a TrafficRank CSV key
    key = 'TRAFFIC_RANK'

    # and a month range
    months = 2

    # when I extract the TrafficHistory data uptrend
    mock_time.gmtime.return_value = time.gmtime(1501820618)
    received = domain_intel.Reporter.get_traffic_trends(data,
                                                        months=months,
                                                        key=key,
                                                        downtrend=False)

    # then I should receive a delta value
    msg = 'Up-trend rank delta response error'
    expected = 117.82
    assert received == expected, msg


def test_get_analyst_qas():
    """Get Analyst QAs data.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I extract the Analyst QAs data
    reporter = domain_intel.Reporter(data=record)
    received = reporter.get_analyst_qas()

    # then I should receive a list of TrafficHistory records
    msg = 'Graph Analyst QAs data error'
    expected = {
        'ANALYST_QAS_DATE': 1500422400,
        'DOMAIN_DOWN_OR_PARKED': 'true',
        'HAS_FORUM_OR_COMMENTS': 'false',
        'HAS_RSS_FEED': 'false',
        'LINKS_TO_OSP': 'false',
        'LINKS_TO_TORRENTS': 'false',
        'P2P_MAGNET_LINKS': 'false',
        'REQUIRES_LOGIN': 'false',
        'SEARCH_FEATURE': 'false'
    }
    assert received == expected, msg


@mock.patch('domain_intel.reporter.domain_intel.utils.time')
def test_dump_wide_column_csv(mock_time):
    """Dump wide column CSV.
    """
    # Given a Domain Intel graph record
    with io.open(os.path.join(SAMPLE_FILE_DIR, 'graph_record.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I dump as CSV
    reporter = domain_intel.Reporter(data=record)
    mock_time.gmtime.return_value = time.gmtime(1498867200)
    received = reporter.dump_wide_column_csv()

    # then I should receive a CSV output
    msg = 'Wide column CSV dump error'
    with io.open(os.path.join('domain_intel',
                              'test',
                              'files',
                              'results',
                              'gbq.csv'), encoding='utf-8') as _fh:
        expected = _fh.read().rstrip()
    assert '\n'.join(received) == expected, msg


def test_dump_wide_column_csv_simple_domain():
    """Dump wide column CSV: simple domain.
    """
    # Given a Domain Intel graph record with no ancillary data
    with io.open(os.path.join(SAMPLE_FILE_DIR,
                              'graph_record_simple_domain.json'),
                 encoding='utf-8') as _fh:
        record = json.loads(_fh.read().rstrip())

    # when I dump as CSV
    reporter = domain_intel.Reporter(data=record)
    received = reporter.dump_wide_column_csv()

    # then I should receive a CSV output
    msg = 'Wide column CSV dump of simple domain error'
    expected = ('cheapdressonsale.com,"cheapdressonsale.com/",,,,False,,,,,'
                ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,0,0,0,0,0,0,0,0,,,,,,,,,')
    assert '\n'.join(received) == expected, msg
