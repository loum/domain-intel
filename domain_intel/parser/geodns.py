""":class:`domain_intel.parser.GeoDNS`
"""
import json
import unicodedata

from logga import log


class GeoDNS(object):
    """

    .. attribute:: domain
        name of the top level domain under consideration

    .. attribute:: raw_data
        the raw, flattened GeoDNS data

    .. attribute:: ipv4
        list of A records from the flattened GeoDNS data

    """
    def __init__(self, raw_data):
        """Break down *raw_data* JSON construct into various components
        that can be used to persist records in the Domain Intel data model.

        """
        self.__raw = raw_data
        self.__data = json.loads(self.__raw)
        self.__domain = GeoDNS._get_domain(self.__data)
        self.__dns_results = self.__data.get('dns_results', {})
        self.__geog_results = self.__data.get('geog_results', {})

    @property
    def domain(self):
        """:attr:`domain`
        """
        return self.__domain

    @property
    def raw(self):
        """:attr:`raw`
        """
        return self.__raw

    @staticmethod
    def _get_domain(data):
        """Extract the domain name from the GeoDNS data construct.
        """
        dns_results = data.get('dns_results', {})
        domain = set([x.get('domain') for x in dns_results.values()])
        if domain:
            domain = unicodedata.normalize('NFKD', next(iter(domain)))

        return domain

    def db_geodns_raw(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "geodns" vertex collection.

        """
        kwargs = {
            '_key': self.domain,
            'data': self.__raw,
        }

        return kwargs

    @property
    def db_ipv4_vertex(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "ipv4" vertex collection.

        """
        return self._db_ip_vertex('A')

    @property
    def db_ipv4_edge(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "ipv4_resolves" edge collection.

        """
        edge_records = []

        for vertex in self._db_ip_vertex('A'):
            kwargs = {
                '_key': '{}:{}'.format(self.domain, vertex.get('_key')),
                '_from': 'domain/{}'.format(self.domain),
                '_to': 'ipv4/{}'.format(vertex.get('_key')),
            }
            edge_records.append(kwargs)

        return edge_records

    @property
    def db_ipv6_vertex(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "ipv6_resolves" vertex collection.

        """
        return self._db_ip_vertex('AAAA')

    def _db_ip_vertex(self, record):
        records = []
        edge_records = []
        ip_addrs = []

        for dns_result in self.__dns_results.values():
            ip_addrs.extend(dns_result.get(record))

        for ip_addr in sorted(list(set(ip_addrs))):
            kwargs = {
                '_key': ip_addr,
            }
            kwargs.update(self.__geog_results.get(ip_addr, {}))
            records.append(kwargs)

        return records

    @property
    def db_ipv6_edge(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "ipv6_resolves" edge collection.

        """
        edge_records = []

        for vertex in self._db_ip_vertex('AAAA'):
            kwargs = {
                '_key': '{}:{}'.format(self.domain, vertex.get('_key')),
                '_from': 'domain/{}'.format(self.domain),
                '_to': 'ipv6/{}'.format(vertex.get('_key')),
            }
            edge_records.append(kwargs)

        return edge_records
