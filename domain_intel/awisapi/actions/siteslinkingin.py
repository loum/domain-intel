"""AWIS API :class:`domain_intel.awisapi.actions.SitesLinkingIn`
"""
from future.moves.urllib.parse import quote
from logga import log

import domain_intel.awisapi


class SitesLinkingIn(domain_intel.awisapi.AwisApi):
    """Alexa Web Information Service API SitesLinkingIn action abstraction.

    Sample usage::

        >>> import domain_intel.awisapi.actions as api
        >>> action = api.SitesLinkingIn(<access_id>, <secret_access_key>)
        api.sites_linking_in('google.com.au', ['Rank', 'LinksInCount'])
        '<?xml version="1.0"?>\n<aws:SitesLinkingIn
        ...
        <aws:StatusCode>Success</aws:StatusCode> ...'

    """
    def sites_linking_in(self, domain, start):
        """Wrapper around the Alexa AWIS SitesLinkingIn action.

        The SitesLinkingIn action returns a list of web sites linking to a
        given web site *domain*.

        Returns:
            the Alexa API reponse string

        """
        params = {'Action': 'SitesLinkingIn'}
        params.update(SitesLinkingIn.build_query(domain, start))

        return self.request(self.build_url(params))

    @staticmethod
    def build_query(domain, start=0):
        """Build the domain query list based on *domain*.
        *domain* is a string value representing the name of the site
        to search.

        Domain names are case sensitive.

        Returns:
            dictionary that captures the the required `SitesLinkingIn` AWIS
            action's query parameters

        """
        url_params = {
            'Url': quote(domain),
            'ResponseGroup': 'SitesLinkingIn',
            'Count': 20,
            'Start': start,
        }

        return url_params
