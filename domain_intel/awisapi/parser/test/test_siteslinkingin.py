""":class:`domain_intel.awisapi.parser.SitesLinkingIn` unit test cases.

"""
import os

import domain_intel.awisapi.parser


def test_siteslinkedin_init():
    """Initialise a domain_intel.awisapi.parser.SitesLinkingIn object.
    """
    # When I initialise a domain_intel.awisapi.parser.SitesLinkingIn object
    sli = domain_intel.awisapi.parser.SitesLinkingIn(None)

    # then I should get a domain_intel.awisapi.parser.SitesLinkingIn instance
    msg = 'Object is not a domain_intel.awisapi.parser.SitesLinkingIn instance'
    assert isinstance(sli, domain_intel.awisapi.parser.SitesLinkingIn), msg

def test_xml_to_json():
    """Convert raw Alexa SitesLinkingIn action result to JSON.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'real_alexa_siteslinkingin.xml')) as _fh:
        source_xml = _fh.read().rstrip().encode('utf-8')

    # and I convert to JSON
    sli = domain_intel.awisapi.parser.SitesLinkingIn(source_xml)

    # then I should get a JSON construct
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'results',
                           'real_alexa_siteslinkingin.json')) as _fh:
        expected = _fh.read().rstrip()
    msg = 'SitesLinkingIn XML to JSON error'
    assert sli.as_json == expected, msg


def test_xml_to_json_empty_string():
    """Convert raw Alexa SitesLinkingIn action result to JSON.
    """
    # When I convert an empty source to JSON
    sli = domain_intel.awisapi.parser.SitesLinkingIn(None)

    # then I should get a JSON construct
    msg = 'SitesLinkingIn XML to JSON error'
    assert sli.as_json is None, msg


def test_extract_titles():
    """Extract titles from Alexa SitesLinkingIn action result.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'real_alexa_siteslinkingin.xml')) as _fh:
        source_xml = _fh.read().rstrip().encode('utf-8')

    # when I extract the SitesLinkingIn titles
    sli = domain_intel.awisapi.parser.SitesLinkingIn(source_xml)
    received = sli.extract_titles()

    # then I should get a JSON construct
    msg = 'SitesLinkingIn titles error'
    expected = [
        {
            'title': 'kaskus.co.id',
            'url': 'archive.kaskus.co.id:80/thread/13385296/1'
        },
        {
            'title': 'secureserver.net',
            'url': 'ip-173-201-142-193.ip.secureserver.net:80/alexa/Alexa_25.html'
        },
        {
            'title': 'blogspot.tw',
            'url': 'redmotion.blogspot.tw:80/2008/10/new-ice-compounds.html'
        },
        {
            'title': 'cocolog-nifty.com',
            'url': 'eurobeter-gc8.cocolog-nifty.com:80/blog/2006/07/post_5b50.html'
        },
        {
            'title': 'blogspot.pe',
            'url': 'streptococcuspyogenes.blogspot.pe:80/2006/06/introduccin_15.html'
        },
        {
            'title': 'blogspot.cl',
            'url': 'streptococcuspyogenes.blogspot.cl:80/2006/06/introduccin_15.html'},
        {
            'title': 'scoop.it',
            'url': 'scoop.it:80/t/confbosimptoude/p/4075409398/2017/02/17/razer-nostromo-software-download-chip?'
        },
        {
            'title': 'telenet.be',
            'url': 'users.telenet.be:80/alenkin/archive.html'
        },
        {
            'title': 'findeen.com',
            'url': 'be.findeen.com:80/8_mile_watch_online_subs.html'
        },
        {
            'title': 'blogspot.ch',
            'url': 'acrossborders.blogspot.ch:80/2006/10/can-you-ever-say-nigger-without-making.html'
        },
        {
            'title': 'startpagina.nl',
            'url': 'startpagina.nl:80/v/overig/vraag/29532/lettertype-gebruiken-ondertiteling'
        },
        {
            'title': 'amara.org',
            'url': 'amara.org:80/en/videos/G5NFTlUp42ul/hai/1657138/4759909'
        },
        {
            'title': 'subtitleseeker.com',
            'url': 'subtitleseeker.com:80/Download-movie-1000292/Its%20a%20Mad%20Mad%20Mad%20Mad%20World%201963-NTSC'
        },
        {
            'title': 'napiprojekt.pl',
            'url': 'forum.napiprojekt.pl:80/viewtopic.php?t=149'
        },
        {
            'title': 'infonu.nl',
            'url': 'pc-en-internet.infonu.nl:80/tutorials/31155-ondertiteling-onder-film-zetten.html'
        },
        {
            'title': 'skynet.be',
            'url': 'users.skynet.be:80/nedsites/film.html'
        },
        {
            'title': 'bsplayer.com',
            'url': 'forum.bsplayer.com:80/general-talk-support/6000-read-first-before-posting.html'
        },
        {
            'title': 'dmoztools.net',
            'url': 'dmoztools.net:80/World/Nederlands/Computers/Multimedia/Beeld_en_Video'
        },
        {
            'title': 'aljyyosh.com',
            'url': 'aljyyosh.com:80/vb/showthread.php?t=12598'
        },
        {
            'title': 'stuffgate.com',
            'url': 'stuffgate.com:80/stuff/website/top-113000-sites'
        }
    ]
    assert received == expected, msg


def test_extract_titles_empty_response():
    """Extract titles from an empty Alexa SitesLinkingIn action result.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'empty_alexa_siteslinkingin.xml')) as _fh:
        source_xml = _fh.read().rstrip().encode('utf-8')

    # when I extract the SitesLinkingIn titles
    sli = domain_intel.awisapi.parser.SitesLinkingIn(source_xml)
    received = sli.extract_titles()

    # then I should get a JSON construct
    msg = 'SitesLinkingIn empty response should produce zeor titles'
    expected = []
    assert received == expected, msg


def test_extract_titles_single_response():
    """Extract titles from an single Alexa SitesLinkingIn action result.
    """
    # When I initialise an Alexa Web Information object
    with open(os.path.join('domain_intel',
                           'test',
                           'files',
                           'samples',
                           'single_alexa_siteslinkingin.xml')) as _fh:
        source_xml = _fh.read().rstrip().encode('utf-8')

    # when I extract the SitesLinkingIn titles
    sli = domain_intel.awisapi.parser.SitesLinkingIn(source_xml)
    received = sli.extract_titles()

    # then I should get a JSON construct
    msg = 'SitesLinkingIn empty response should produce zeor titles'
    expected = [
        {
            'title': 'votrian.blogspot.com',
            'url': 'votrian.blogspot.com:80/2008/06/eve-areas-of-interest-aoi.html'
        }
    ]
    assert received == expected, msg
