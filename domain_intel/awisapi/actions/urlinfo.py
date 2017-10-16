"""AWIS API :class:`domain_intel.awisapi.actions.UrlInfo`
"""
from future.moves.urllib.parse import quote
from logga import log

import domain_intel.awisapi

RESPONSE_GROUPS = {
    'url_info': [
        'RelatedLinks',
        'Categories',
        'Rank',
        'RankByCountry',
        'UsageStats',
        'AdultContent',
        'Speed',
        'Language',
        'OwnedDomains',
        'LinksInCount',
        'SiteData',
    ]
}


class UrlInfo(domain_intel.awisapi.AwisApi):
    """Alexa Web Information Service API UrlInfo action abstraction.

    Sample usage::

        import domain_intel
        api = domain_intel.awisapi.actions.UrlInfo(<access_id>,
                                                    <secret_access_key>)
        api.url_info('google.com.au', ['Rank', 'LinksInCount'])
        '<?xml version="1.0"?>\n<aws:UrlInfoResponse
        ...
        <aws:StatusCode>Success</aws:StatusCode>...'

    """
    def __init__(self, access_id, secret_access_key):
        super(UrlInfo, self).__init__(access_id, secret_access_key)

        self.__response_groups = RESPONSE_GROUPS

    def url_info(self, domains, response_groups=None):
        """Wrapper around the Alexa AWIS UrlInfo action.

        A UrlInfo gets information about pages and sites on the web,
        their traffic, content, and related sites.

        **Args**:
            *domains*: either a string value representing the name of the
            domain to search or a list of domain names.

            *response_groups*: see the `RESPONSE_GROUPS` global for
            the supporting sub-sets of groups that build the `url_info`
            action response.  By default, all groups are included in
            Alexa query

        **Returns:**
            the Alexa API reponse string

        """
        log.info('Domains to search: "%s"', domains)
        if response_groups is None:
            response_groups = self.__response_groups.get('url_info')

        params = {'Action': 'UrlInfo'}

        params.update(UrlInfo.build_domain_query(domains,
                                                 response_groups))

        return self.request(self.build_url(params))

    @staticmethod
    def build_domain_query(domains, response_groups):
        """Build the domain query list based on *domains*.

        The query parameters produced vary based on whether the *domains*
        parameter is a string or list of strings.

        Domain names are case sensitive.

        Args:
            *domains*: either a string value representing the name of the
            domain to search or a list of domain names.

            *response_groups*: see the `RESPONSE_GROUPS` global for
            the supporting sub-sets of groups that build the `url_info`
            action response.  By default, all groups are included in
            Alexa query

        Returns:
            dictionary that captures the the required `UrlInfo` AWIS
            action's query parameters

        """
        domains_url = None

        if not isinstance(domains, (list, tuple)):
            domains_url = {
                'Url': quote(domains),
                'ResponseGroup': ','.join(response_groups),
            }
        else:
            domains_url = {
                'UrlInfo.Shared.ResponseGroup': ','.join(response_groups)
            }
            for i, url in enumerate(domains):
                domains_url['UrlInfo.{}.Url'.format(i + 1)] = quote(url)

        return domains_url
