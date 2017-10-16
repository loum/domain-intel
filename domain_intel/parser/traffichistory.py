""":class:`domain_intel.parser.TrafficHistory`
"""


class TrafficHistory(object):
    """

    .. attribute:: domain
        name of the top level domain under consideration

    """
    def __init__(self, raw_data):
        """Break down the *raw_data* JSON construct into various components
        that can be used to persist records in the Domain Intel data model.

        """
        self.__data = raw_data
        self.__domain = TrafficHistory._get_domain(self.__data)
        self.__start = TrafficHistory._get_start(self.__data)

    @property
    def data(self):
        """:attr:`data`
        """
        return self.__data

    @property
    def domain(self):
        """:attr:`domain`
        """
        return self.__domain

    @property
    def start(self):
        """:attr:`start`
        """
        return self.__start

    @staticmethod
    def _get_domain(data):
        """Extract the domain name from the TrafficHistory data construct.
        """
        return data['TrafficHistory']['Site']['$']

    @staticmethod
    def _get_start(data):
        """Extract the traffic start from the TrafficHistory data construct.
        """
        return data['TrafficHistory']['Start']['$']

    def db_traffichistory_raw(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "traffic" vertex collection.

        """
        kwargs = {
            '_key': '{}:{}'.format(self.domain, self.start),
            'data': self.data,
        }

        return kwargs

    def db_visit_edge(self):
        """Return the data structure consistent with ingest into the
        ArangoDB "visit" edge collection.

        """
        kwargs = {
            '_key': '{}:{}'.format(self.domain, self.start),
            '_from': 'traffic/{}:{}'.format(self.domain, self.start),
            '_to': 'domain/{}'.format(self.domain),
        }

        return kwargs
