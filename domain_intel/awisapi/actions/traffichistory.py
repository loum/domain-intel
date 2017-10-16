"""AWIS API :class:`domain_intel.awisapi.actions.TrafficHistory`
"""
import datetime
from future.moves.urllib.parse import quote
import calendar
from dateutil.relativedelta import relativedelta
from logga import log

import domain_intel.awisapi


class TrafficHistory(domain_intel.awisapi.AwisApi):
    """Alexa Web Information Service API SitesLinkingIn action abstraction.

    Sample usage::

        >>> import domain_intel.awisapi.actions as api
        >>> action = api.TrafficHistory(<access_id>, <secret_access_key>)
        >>> action.traffic_history('google.com.au')
        '<?xml version="1.0"?>\n<aws:TrafficHistoryResponse ...
        ...
        <aws:StatusCode>Success</aws:StatusCode> ...'

    """
    def traffic_history(self, domain):
        """Wrapper around the Alexa AWIS TrafficHistory action.

        The TrafficHistory action returns the daily Alexa Traffic Rank,
        Reach per Million Users and Unique Page Views per Million Users
        for each day going back 4 years.  Sites with a rank in excess of
        1,000,000 are not included.

        Returns:
            the Alexa API reponse string

        """
        params = {'Action': 'TrafficHistory'}
        params.update(TrafficHistory.build_query(domain))
        log.info('Alexa TrafficHistory request for domain: "%s"', domain)

        return self.request(self.build_url(params))

    @staticmethod
    def build_query(domain, month_range=0):
        """Build the domain query list based on *domain*.
        *domain* is a string value representing the name of the site
        to search.

        Domain names are case sensitive.

        Returns:
            dictionary that captures the the required `TrafficHistory` AWIS
            action's query parameters for the last month.

        """
        today = datetime.date.today()
        first_day = today.replace(day=1)
        last_month = first_day - datetime.timedelta(days=1)
        if month_range:
            last_month = last_month - relativedelta(months=month_range)
        last_day = calendar.monthrange(last_month.year, last_month.month)[1]
        start = '{}01'.format(last_month.strftime('%Y%m'))

        url_params = {
            'Url': quote(domain),
            'ResponseGroup': 'History',
            'Range': last_day,
            'Start': start,
        }

        return url_params
