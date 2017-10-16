""":class:`domain_intel.awisapi.parser.UrlInfo` unit test cases.

"""
import os

import domain_intel.awisapi.parser


def test_url_info():
    """Initialise a domain_intel.awisapi.parser.UrlInfo object.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record.json')) as _fh:
        data = _fh.read().rstrip().encode('utf-8')
    url_info = domain_intel.awisapi.parser.UrlInfo(data)

    # then I should get a domain_intel.awisapi.parser.UrlInfo instance
    msg = 'Object is not a domain_intel.awisapi.parser.UrlInfo instance'
    assert isinstance(url_info, domain_intel.awisapi.parser.UrlInfo), msg

    # and the "domain" attribute should return a value
    msg = 'UrlInfo.domain error'
    assert url_info.domain == 'feedblitz.com', msg

    # and the "online_since" attribute should return a value
    msg = 'UrlInfo.online_since error'
    assert url_info.online_since == '16-May-2005', msg

    # and the "median_load_time" attribute should return a value
    msg = 'UrlInfo.median_load_time error'
    assert url_info.median_load_time == 1969, msg

    # and the "speed_percentile" attribute should return a value
    msg = 'UrlInfo.speed_percentile error'
    assert url_info.speed_percentile == 43, msg

    # and the "adult_content" attribute should return a value
    msg = 'UrlInfo.adult_content error'
    assert not url_info.adult_content, msg

    # and the "links_in_count" attribute should return a value
    msg = 'UrlInfo.links_in_count error'
    assert url_info.links_in_count == 15943, msg

    # and the "locale" attribute should return a value
    msg = 'UrlInfo.locale error'
    assert url_info.locale == 'en', msg

    # and the "encoding" attribute should return a value
    msg = 'UrlInfo.encoding error'
    assert url_info.encoding == 'us-ascii', msg

    # and the "domain_title" attribute should return a value
    msg = 'UrlInfo.domain_title error'
    expected = 'FeedBlitz: Add Email to your Blog and RSS Feeds'
    assert url_info.domain_title == expected, msg

    # and the "domain_description" attribute should return a value
    msg = 'UrlInfo.domain_description error'
    expected = 'FeedBlitz is a service that monitors blogs'
    assert url_info.domain_description[:42] == expected, msg

    # and the "domain_rank" attribute should return a value
    msg = 'UrlInfo.domain_rank error'
    assert url_info.domain_rank == 53960, msg

    # and the "domain_country_rank" attribute should return a value
    msg = 'UrlInfo.domain_country_rank error'
    expected = [
        ('IN', 29491),
        ('SG', 44897),
        ('PH', 27847),
        ('ES', 65628),
        ('US', 19476),
        ('IR', 95352),
        ('AU', 73713),
        ('NG', 9685),
        ('GB', 29143),
        ('PT', 20442),
        ('BR', 75169),
        ('IT', 74487),
        ('ZA', 48114),
        ('CA', 39291),
        ('JP', 82327),
        ('DE', 100216),
    ]
    assert list(url_info.domain_rank_by_country) == expected, msg

    # and the "related_links" attribute should return a value
    msg = 'UrlInfo.related_links count error'
    assert len(list(url_info.related_links)) == 10, msg

    msg = 'UrlInfo.related_links value error'
    expected = (
         'www.writingwhitepapers.com/',
         'http://www.writingwhitepapers.com/',
         'Writing White Papers'
    )
    assert list(url_info.related_links)[3] == expected, msg

    # and the "contributing_subdomains" attribute should return a value
    msg = 'UrlInfo.contributing_subdomains count error'
    assert len(list(url_info.contributing_subdomains)) == 7, msg

    msg = 'UrlInfo.contributing_subdomains value error'
    expected = ('feeds.feedblitz.com', 1, 8.97, 4.58, 1.1)
    assert list(url_info.contributing_subdomains)[3] == expected, msg

    # and domain state should be created
    msg = 'UrlInfo domain state error'
    expected = {
        'title': 'FeedBlitz: Add Email to your Blog and RSS Feeds',
        'online_since': '16-May-2005',
        'median_load_time': 1969,
        'speed_percentile': 43,
        'adult_content': False,
        'links_in_count': 15943,
        'locale': 'en',
        'encoding': 'us-ascii',
        'description':
            ('FeedBlitz is a service that monitors blogs, RSS feeds and '
             'Web URLs to provide greater reach for feed publishers. '
             'FeedBlitz takes all the headache out of converting feed and '
             'blog updates into email digests, delivered daily to '
             'subscribers inboxes. FeedBlitz manages subscriptions, '
             'circulation tracking, testing, and is compatible with all '
             'major blogging platforms and services such as Blogger, '
             'Typepad and FeedBurner.'),
        'rank': 53960,
    }
    assert url_info() == expected, msg

def test_url_info_simple():
    """Initialise a domain_intel.awisapi.parser.UrlInfo simple object.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'kafka_consumer_record_simple.json')) as _fh:
        data = _fh.read().rstrip().encode('utf-8')
    url_info = domain_intel.awisapi.parser.UrlInfo(data)

    # I should get a domain_intel.awisapi.parser.UrlInfo instance
    msg = 'Object is not a domain_intel.awisapi.parser.UrlInfo instance'
    assert isinstance(url_info, domain_intel.awisapi.parser.UrlInfo), msg

    # and the state should be created
    msg = 'UrlInfo.domain error'
    assert url_info.domain == 'allmp3s.xyz', msg

    # and the "online_since" attribute should return a value
    msg = 'UrlInfo.online_since error'
    assert url_info.online_since is None, msg

    # and the "median_load_time" attribute should return a value
    msg = 'UrlInfo.median_load_time error'
    assert url_info.median_load_time is None, msg

    # and the "speed_percentile" attribute should return a value
    msg = 'UrlInfo.speed_percentile error'
    assert url_info.speed_percentile is None, msg

    # and the "adult_content" attribute should return a value
    msg = 'UrlInfo.adult_content error'
    assert not url_info.adult_content, msg

    # and the "links_in_count" attribute should return a value
    msg = 'UrlInfo.links_in_count error'
    assert url_info.links_in_count == 11, msg

    # and the "locale" attribute should return a value
    msg = 'UrlInfo.locale error'
    assert url_info.locale is None, msg

    # and the "encoding" attribute should return a value
    msg = 'UrlInfo.encoding error'
    assert url_info.encoding is None, msg

    # and the "domain_title" attribute should return a value
    msg = 'UrlInfo.domain_title error'
    expected = 'allmp3s.xyz/'
    assert url_info.domain_title == expected, msg

    # and the "domain_description" attribute should return a value
    msg = 'UrlInfo.domain_description error'
    assert url_info.domain_description is None, msg

    # and the "domain_rank" attribute should return a value
    msg = 'UrlInfo.domain_rank error'
    assert url_info.domain_rank == 2115403, msg

    # and the "domain_country_rank" attribute should return a value
    msg = 'UrlInfo.domain_country_rank error'
    expected = [
        ('IN', 365525),
        ('BD', 73422)
    ]
    assert list(url_info.domain_rank_by_country) == expected, msg

    # and the "related_links" attribute should return a value
    msg = 'UrlInfo.related_links count error'
    assert len(list(url_info.related_links)) == 0, msg

    # and the "contributing_subdomains" attribute should return a value
    msg = 'UrlInfo.contributing_subdomains count error'
    assert len(list(url_info.contributing_subdomains)) == 1, msg

    msg = 'UrlInfo.contributing_subdomains value error'
    expected = ('allmp3s.xyz', 1, 100.0, 100.0, 2)
    assert list(url_info.contributing_subdomains)[0] == expected, msg

    # and domain state should be created
    msg = 'UrlInfo domain state error'
    expected = {
        'title': 'allmp3s.xyz/',
        'online_since': None,
        'median_load_time': None,
        'speed_percentile': None,
        'adult_content': False,
        'links_in_count': 11,
        'locale': None,
        'encoding': None,
        'description': None,
        'rank': 2115403,
    }
    assert url_info() == expected, msg
