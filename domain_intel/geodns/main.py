from __future__ import print_function, unicode_literals
from future.utils import raise_from
from past.builtins import basestring
from domain_intel.geodns import CompassHTTPResolver, CheckHostNet, CheckHostNetResult, CompassServerEmptyResponse
import json
import six
from logga import log


class GeoDNSError(Exception):
    pass


def initialise_network_attributes(func):
    """return decorator that calls initialise_network_attributes on object that func is bound to"""
    def _initialiser(self, *args, **kwargs):
        self.initialise_network_attributes()
        return func(self, *args, **kwargs)
    return _initialiser


class GeoDNS(object):
    """geographically dispersed DNS results for a single domain"""

    def __init__(
            self,
            compass_username,
            compass_password,
            compass_resolver=None,
            compass_resolver_factory=None,
            dns_resolver=None,
            dns_resolver_factory=None,
    ):
        self.compass_username = compass_username
        self.compass_password = compass_password
        self.compass_resolver = compass_resolver
        self.compass_resolver_factory = compass_resolver_factory
        self.dns_resolver = dns_resolver
        self.dns_resolver_factory = dns_resolver_factory

    @initialise_network_attributes
    def resolve_dns(self, domain):
        """main geodns function, performs full geodns functionality"""
        # TODO: max_nodes should be a configuration variable.

        if isinstance(domain, six.binary_type):
            domain = domain.decode("utf-8")

        dns_results = self.dns_resolver.resolve_dns(domain=domain, max_nodes=10)
        return dns_results

    @initialise_network_attributes
    def resolve_geog_from_dns(self, dns_results):

        # handle serialised and deserialised dns_results
        if isinstance(dns_results, basestring):
            try:
                dns_results = ParsedDNS.from_serialised(dns_results)
            except Exception as exc:
                raise_from(GeoDNSError("couldn't deserialise byte string encoded dns_results: %s" % dns_results,), exc)

        geog_results = {}
        # extract unique IPv4 addresses from dns_results
        for a_record in {
            a
            for dns_resultset in dns_results.values()
            for a in dns_resultset["A"]
        }:
            try:
                geog_results[a_record] = self.compass_resolver.resolve(a_record)
            except CompassServerEmptyResponse as emptyExc:
                log.warning("got empty compass results for %s, leaving blank", a_record)
                geog_results = {}

        return ParsedGeoDNS(dns_results=dns_results, geog_results=geog_results)

    @staticmethod
    def parse_checkhostnetresult(result):
        """return parsed and validated response representing the combination of check and result results"""

        # handle serialised or deserialised result
        if isinstance(result, basestring):
            try:
                result = CheckHostNetResult.from_serialised(result)
            except Exception as exc:
                raise_from(GeoDNSError("couldn't deserialise byte string encoded checkhostnet results: %s" % result,), exc)

        _parsed_check_result = result.parsed_check_result()
        _parsed_results_result = result.parsed_results_result()

        results = ParsedDNS()
        for node_name in _parsed_check_result["nodes"].keys():
            country_id = _parsed_check_result["nodes"][node_name][0]
            if country_id not in results:
                results[country_id] = {
                    "domain": result.domain,
                    "country_id": country_id,
                    "A": [],
                    "AAAA": [],
                }

            # apparently node_name might not have results
            # maybe this is when one node fails to lookup or smth
            if _parsed_results_result.get(node_name) is not None:
                for node_result in _parsed_results_result[node_name]:
                    # TODO: it is unclear why there would be failures here. SUPPORTDIS-10
                    if not isinstance(results[country_id]["A"], list):
                        log.warning("A record list for country_id %s empty, raw input: %s", country_id, result)
                        results[country_id]["A"] = []
                    if not isinstance(results[country_id]["AAAA"], list):
                        log.warning("AAAA record list for country_id %s empty, raw input: %s", country_id, result)
                        results[country_id]["AAAA"] = []

                    if node_result is None:
                        log.warning("got NoneType node_result for key %s: %s", node_name, result)
                    else:
                        if "A" in node_result:
                            results[country_id]["A"].extend(node_result["A"])
                        else:
                            log.warning("got no A results for raw input: %s", result)
                        if "AAAA" in node_result:
                            results[country_id]["AAAA"].extend(node_result["AAAA"])
                        else:
                            log.warning("got no AAAA results for raw input: %s", result)

        return results

    def initialise_network_attributes(self):
        """ensures that all network attributes are fully intialised. if forking, ensure this is called
        AFTER any forking, else filehandles will be shared unsafely"""

        if self.compass_resolver is None:
            if self.compass_resolver_factory is not None:
                self.compass_resolver = self.compass_resolver_factory()
            else:
                self.compass_resolver = CompassHTTPResolver(
                    username=self.compass_username,
                    password=self.compass_password,
                )

        if self.dns_resolver is None:
            if self.dns_resolver_factory is not None:
                self.dns_resolver = self.dns_resolver_factory()
            else:
                self.dns_resolver = CheckHostNet()


class ParsedDNS(dict):
    """bit of a cop out. wraps the parsed dns results coming from parse_checkhostnetresult.
    doesnt really provide any functionality other than packing/unpacking"""
    def marshal(self):
        """return self as byte string of json encoding"""
        return json.dumps(self).encode("utf-8")

    @classmethod
    def from_serialised(cls, serialised):
        """load from serialised json bytestring, as serialised by marshal()"""
        raw = json.loads(serialised.decode("utf-8"))
        return cls(**raw)


class ParsedGeoDNS(ParsedDNS):
    """see ParsedDNS"""
    pass
