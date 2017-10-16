""":class:`SitesLinkingIn`

"""
import json
import collections
import lxml.etree
import xmljson


NS_20050711 = 'http://awis.amazonaws.com/doc/2005-07-11'
NS_20051005 = 'http://alexa.amazonaws.com/doc/2005-10-05/'
NS = {'a': NS_20051005, 'b': NS_20050711}


class SitesLinkingIn(object):
    """Abstacts an AWIS SitesLinkingIn action.

    .. attributes:: xml

    """
    def __init__(self, raw_xml):
        """Parse and de-construct a *raw_xml* record.

        """
        if raw_xml is None:
            raw_xml = b''
        self.__xml = raw_xml.decode('utf-8')
        self.__as_json = None

    def __call__(self):
        """Just dump the raw JSON.
        """
        return {}

    @property
    def xml(self):
        """Just dump the raw XML.
        """
        return self.__xml

    @property
    def as_json(self):
        """:attr:`raw` XML converted to JSON.
        """
        if self.__as_json is None and self.xml:
            self._xml_to_json()

        return self.__as_json

    def _xml_to_json(self):
        """Convert raw Alexa SitesLinkingIn action result to JSON.

        """
        root = lxml.etree.fromstring(self.xml)
        xpath = ('//a:SitesLinkingInResponse/'
                 'b:Response/'
                 'b:SitesLinkingInResult')
        sites = root.xpath(xpath, namespaces=NS)
        if sites:
            bf_json = xmljson.BadgerFish(dict_type=collections.OrderedDict)
            tmp = json.dumps(bf_json.data(sites[0]))
            ns_replace = r'{{{0}}}'.format(NS_20050711)
            self.__as_json = tmp.replace(ns_replace, '')
        else:
            log.error('Unable to parse SitesLinkingIn XML: %s', self.xml)

    def extract_titles(self):
        """Extract titles from the Alexa SitesLinkingInResult action
        response.

        """
        titles = []
        if self.as_json is not None:
            base = json.loads(self.as_json)['SitesLinkingInResult']['Alexa']
            sites_linking_in = base['SitesLinkingIn']
            sites = sites_linking_in.get('Site', [])

            if isinstance(sites, list):
                for site in sites_linking_in.get('Site', []):
                    title = site.get('Title').get('$')
                    url = site.get('Url').get('$')
                    titles.append({'title': title, 'url': url})
            else:
                kwargs = {
                    'title': sites.get('Title').get('$'),
                    'url': sites.get('Url').get('$'),
                }
                titles.append(kwargs)

        return titles
