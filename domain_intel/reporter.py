""":class:`Reporter`

"""
from __future__ import division
from builtins import str
import re
import collections
from logga import log

import domain_intel.utils


CONFIG = domain_intel.common.CONFIG
COUNTRY_CODES = domain_intel.common.COUNTRY_CODES

DOMAIN_KEYS = {
    '_key': 'DOMAIN',
    'title': None,
    'online_since': None,
    'median_load_time': None,
    'speed_percentile': None,
    'adult_content': None,
    'links_in_count': None,
    'locale': None,
    'encoding': None,
    'description': None,
    'rank': None,
}

COUNTRY_RANK_KEYS = {
    '_to': 'DOMAIN',
    'label': 'COUNTRY_RANK',
}

RELATED_KEYS = collections.OrderedDict([
    ('label', 'related_domain'),
])

LINK_KEYS = collections.OrderedDict([
    ('navigable_url', None),
    ('title', None),
])


class Reporter(object):
    """
    .. attribute:: raw
        source Domain Intel graph structure that captures
        the relationships between the various assets

    .. attribute:: domain
        dictionary detail of the base domain name

    .. attribute:: vertices
        graph vertices

    .. attribute:: paths
        graph paths

    .. attribute:: domain
        domain graph detail

    """
    def __init__(self, data):
        """*data* is the raw Domain Intel graph structure that captures
        the relationships between the various assets.

        """
        self.__raw = data
        if self.__raw is None:
            self.__raw = {}

        self.__vertices = self.__raw.get('vertices', [])
        self.__paths = self.__raw.get('paths', [])

        self.__domain = {}
        self.__countries = []

    @property
    def raw(self):
        """:attr:`raw`
        """
        return self.__raw

    @property
    def vertices(self):
        """:attr:`vertices`
        """
        return self.__vertices

    @property
    def paths(self):
        """:attr:`paths`
        """
        return self.__paths

    @property
    def domain(self):
        """Domain detail is the start node in the graph dump so we assume
        that it will be the first index in the :attr:`vertices` list.
        """
        if not self.__domain.keys():
            tmp = {}
            if self.vertices:
                tmp = self.vertices[0]

            for quote_token in ['title', 'description']:
                if tmp.get(quote_token) is not None:
                    token_value = u'{}'.format(tmp.get(quote_token) or str())
                    token_value = token_value.replace(u'"', u'""')
                    tmp[quote_token] = u'"{}"'.format(token_value)

            nuller = lambda x: x if x is not None else ''
            csv_map = lambda y: DOMAIN_KEYS[y] if DOMAIN_KEYS[y] is not None else y.upper()
            source_keys = [x for x in DOMAIN_KEYS.keys()]
            self.__domain = {csv_map(k): nuller(tmp[k]) for k in source_keys}

        return self.__domain

    @property
    def countries(self):
        """Country detail includes:
        - rank
        """
        if not self.__countries and self.__vertices:
            for vertice_id in [x.get('_id') for x in self.__vertices]:
                if 'country' in vertice_id:
                    self.__countries.append(vertice_id)

        return self.__countries

    def get_country_ranks(self):
        """Get country ranks from the graph path edges.

        Returns:
            list of country ranks in the form::

                [
                    {'_to': 'country/BE', 'label': 1440},
                    {'_to': 'country/DE', 'label': 45635},
                    {'_to': 'country/NL', 'label': 2500},
                    ...
                ]

        """
        country_ranks = []
        raw_ranks = []

        for graph_path in self.paths:
            for edge in graph_path.get('edges'):
                if 'ranked' not in edge.get('_id'):
                    continue

                source_keys = [x for x in COUNTRY_RANK_KEYS.keys()]
                raw_ranks.append({k: edge.get(k) for k in source_keys})

        for item in raw_ranks:
            code = item.get('_to').split('/')[-1]
            kwargs = {
                'COUNTRY_CODE': code,
                'COUNTRY_NAME': COUNTRY_CODES.get(code),
                'COUNTRY_RANK': item.get('label') or str()
            }
            country_ranks.append(kwargs)

        return country_ranks

    def get_sites_linking_in(self):
        """Get the sites linking into the domain.

        """
        all_sites = []

        for graph_path in self.paths:
            edge = graph_path.get('edges', [])
            vertices = graph_path.get('vertices')

            if edge and 'links_into' in edge[0].get('_id'):
                url = edge[0].get('label')
                url = url.replace(u'"', u'""')
                url = u'"{}"'.format(url)

                for vertice in vertices:
                    if vertice.get('domain_linkingin'):
                        domain_linkingin = vertice.get('domain_linkingin')
                        kwargs = {
                            'URL_LINKINGIN': url,
                            'DOMAIN_LINKINGIN': domain_linkingin,
                        }
                        all_sites.append(kwargs)
                        break

        return all_sites

    def get_geodns(self):
        """Get all the GeoDNS data.
        """
        all_geodns = []

        for graph_path in self.paths:
            edge = graph_path.get('edges', [])
            vertices = graph_path.get('vertices')

            for ipv in ['ipv4']:
                if edge and '{}_resolves'.format(ipv) in edge[0].get('_id'):
                    for vertice in vertices:
                        log.debug('XXX %s', vertices)
                        if vertice and not re.match('{}/'.format(ipv),
                                                    vertice.get('_id')):
                            continue

                        ip_addr = vertice.get('_key')
                        dns_org = vertice.get('organisation', {}).get('name', '')
                        isp = vertice.get('isp', {}).get('name', '')
                        lat = vertice.get('geospatial', {}).get('latitude', '')
                        lng = vertice.get('geospatial', {}).get('longitude', '')
                        country_code = vertice.get('country', {}).get('iso3166_code_2', '')
                        country_name = vertice.get('country', {}).get('name', '')
                        continent_code = vertice.get('continent', {}).get('code', '')
                        continent_name = vertice.get('continent', {}).get('name', '')

                        if dns_org:
                            dns_org = '"{}"'.format(dns_org)

                        if isp:
                            isp = '"{}"'.format(isp)

                        token = ipv.upper()
                        kwargs = {
                            '{}_ADDR'.format(token): ip_addr,
                            '{}_ORG'.format(token): dns_org,
                            '{}_ISP'.format(token): isp,
                            '{}_LATITUDE'.format(token): lat,
                            '{}_LONGITUDE'.format(token): lng,
                            '{}_COUNTRY_CODE'.format(token):
                                country_code,
                            '{}_COUNTRY'.format(token):
                                country_name,
                            '{}_CONTINENT_CODE'.format(token):
                                continent_code,
                            '{}_CONTINENT'.format(token): continent_name,
                        }
                        all_geodns.append(kwargs)

        return all_geodns

    def get_traffichistory(self):
        """Get all the TrafficHistory data.

        """
        all_traffichistory = []

        keys = [
            'TRAFFIC_TS',
            'TRAFFIC_PAGE_VIEWS_PM',
            'TRAFFIC_PAGE_VIEWS_USER',
            'TRAFFIC_RANK',
            'TRAFFIC_REACH',
        ]

        for graph_path in self.paths:
            edge = graph_path.get('edges', [])
            vertices = graph_path.get('vertices')

            if edge and re.match('visit/', edge[0].get('_id')):
                for vertice in vertices:
                    if not re.match('traffic/', vertice.get('_id')):
                        continue

                    for data in Reporter._parse_traffic_history(vertice):
                        all_traffichistory.append(dict(zip(keys, data)))

        return all_traffichistory

    @staticmethod
    def get_traffic_trends(traffic_data,
                           months=0,
                           key='TRAFFIC_PAGE_VIEWS_PM',
                           downtrend=True):
        """Track through *traffic_data* and identify trends across the
        time period *months*.

        As the algorithm can be re-used for multiple CSV column values,
        we can override the default *key* parameter of
        ``TRAFFIC_PAGE_VIEWS_PM`` to specify a different trend context.

        The trend algorithm supports downtrends (default) and uptrends.  By
        setting the *downtrend* parameter to Boolean ``False``, the metrics
        are reversed so that the uptrend delta is calculated instead.

        The algorithm first identifies the highest value of *key*
        and then averages out the remaining values before calculating
        the delta

        Returns:
            the delta value based on the difference between the
            highest value and average of subsequent values

        """
        start_epoch, end_epoch = domain_intel.utils.get_epoch_ranges(months)

        def in_range(number, start, end):
            status = False
            if number >= start and number <= end:
                status = True
            return status

        items = [x for x in traffic_data if in_range(x.get('TRAFFIC_TS'),
                                                     start_epoch,
                                                     end_epoch)]

        # Strip out unwanted values.
        items = [x for x in traffic_data if x.get(key)]

        # Set trend context.
        bound = max
        negator = 1

        if ((key == 'TRAFFIC_PAGE_VIEWS_PM' and not downtrend)
                or (key == 'TRAFFIC_RANK' and downtrend)):
            bound = min
            negator = -1

        # Find extreme value.
        items = sorted(items, key=lambda k: k['TRAFFIC_TS'])
        delta = 0
        if items:
            extreme_index, extreme_item = bound(enumerate(items),
                                                key=lambda x: x[1][key])

            # Sum and average all remaining index values.
            total = sum(x[key] for x in items[(extreme_index + 1):])
            average = total / (len(items) - extreme_index + 1)
            delta = extreme_item[key] - (negator * average)
            delta = float('{0:.2f}'.format(delta))

        return delta

    @staticmethod
    def _parse_traffic_history(data):
        """Helper method that parses the TrafficHistory component
        from *data* taken from the domain traversal snapshot.

        Returns:
            list of TrafficData metrics for an available day

        """
        results = []

        historic_data = data['data']['TrafficHistory']['HistoricalData']
        traffic_data = historic_data.get('Data', [])
        if not isinstance(traffic_data, list):
            traffic_data = [traffic_data]

        for item in traffic_data:
            date = item['Date']['$']
            timestamp = domain_intel.utils.epoch_from_str(date)

            page_views = item.get('PageViews', {})
            page_views_per_million = page_views.get('PerMillion', {}).get('$', '')
            page_views_per_user = page_views.get('PerUser', {}).get('$', '')

            rank = item.get('Rank', {}).get('$', '')

            reach = item.get('Reach', {})
            reach_per_million = reach.get('PerMillion', {}).get('$', '')

            args = [
                timestamp,
                page_views_per_million,
                page_views_per_user,
                rank,
                reach_per_million,
            ]
            results.append(args)

        return results

    def get_analyst_qas(self):
        """Get all the Analyst QAs data.

        """
        def val(flag):
            result = flag
            if (isinstance(flag, str) and
                    (flag.upper() == 'Y' or flag.upper() == 'N')):
                result = str(flag.upper() == 'Y').lower()

            return result

        analyst_qas = {}

        for graph_path in self.paths:
            edge = graph_path.get('edges', [])
            vertices = graph_path.get('vertices')

            if edge and re.match('analyst-qas/', edge[0].get('_to')):
                for vertice in vertices:
                    if not re.match('analyst-qas/', vertice.get('_id')):
                        continue

                    data = vertice.get('data', {})
                    analyst_qas = {a.upper(): val(b) for a, b in data.items()}

        return analyst_qas

    def dump_wide_column_csv(self):
        """Dump the graph into a wide column CSV structure.

        """
        log.info('Reporting on domain "%s"', self.domain.get('DOMAIN'))
        records_dumped = []

        traffic_history = self.get_traffichistory()
        one_month_traffic_dt = self.get_traffic_trends(traffic_history)
        one_month_traffic_ut = self.get_traffic_trends(traffic_history,
                                                       downtrend=False)
        one_month_rank_dt = self.get_traffic_trends(traffic_history,
                                                    key='TRAFFIC_RANK')
        one_month_rank_ut = self.get_traffic_trends(traffic_history,
                                                    key='TRAFFIC_RANK',
                                                    downtrend=False)
        three_month_traffic_dt = self.get_traffic_trends(traffic_history,
                                                         months=2)
        three_month_traffic_ut = self.get_traffic_trends(traffic_history,
                                                         months=2,
                                                         downtrend=False)
        three_month_rank_dt = self.get_traffic_trends(traffic_history,
                                                      months=2,
                                                      key='TRAFFIC_RANK')
        three_month_rank_ut = self.get_traffic_trends(traffic_history,
                                                      months=2,
                                                      key='TRAFFIC_RANK',
                                                      downtrend=False)

        trends = {
            'MNTH_1_VISITS_DT': one_month_traffic_dt,
            'MNTH_1_VISITS_UT': one_month_traffic_ut,
            'MNTH_3_VISITS_DT': three_month_traffic_dt,
            'MNTH_3_VISITS_UT': three_month_traffic_ut,
            'MNTH_1_RANK_DT': one_month_rank_dt,
            'MNTH_1_RANK_UT': one_month_rank_ut,
            'MNTH_3_RANK_DT': three_month_rank_dt,
            'MNTH_3_RANK_UT': three_month_rank_ut,
        }
        domain = self.domain.copy()
        domain.update(trends)
        domain.update(self.get_analyst_qas())

        # Country ranks.
        for country_rank in self.get_country_ranks():
            line_item_data = domain.copy()
            line_item_data.update(country_rank)
            records_dumped.append(Reporter.line(line_item_data))

        # URLs linking in.
        for site_linking_in in self.get_sites_linking_in():
            line_item_data = domain.copy()
            line_item_data.update(site_linking_in)
            records_dumped.append(Reporter.line(line_item_data))

        # GeoDNS.
        for geodns in self.get_geodns():
            line_item_data = domain.copy()
            line_item_data.update(geodns)
            records_dumped.append(Reporter.line(line_item_data))

        # Traffic.
        for traffic in traffic_history:
            line_item_data = domain.copy()
            line_item_data.update(traffic)
            records_dumped.append(Reporter.line(line_item_data))

        # Return the domain data if it doesn't contain
        # ancilliary data.
        if not records_dumped:
            records_dumped.append(Reporter.line(domain))

        return records_dumped

    @staticmethod
    def line(data):
        """Construct a wide-column CSV line item from the
        *data* dictionary.

        Order of CSV values is guaranteed based on
        :class:`domain_intel.GbqCsv`

        Returns:
            a CSV representation of *data*

        """
        line_item = [''] * len(domain_intel.GbqCsv.__members__)

        for key, value in data.items():
            index_obj = getattr(domain_intel.GbqCsv, key)
            line_item[index_obj.value] = value

        return ','.join([u'{}'.format(x) for x in line_item])
