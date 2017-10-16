from __future__ import print_function, unicode_literals
from future.utils import raise_from
import requests
import json
import pickle
from logga import log


class CheckHostNetError(Exception):
    pass


class CheckHostNetResult(object):
    """Holds intermediate representation from check-host.net"""

    def __init__(self, check_result, results_result, domain=""):
        self.check_result = check_result
        self.results_result = results_result
        self.domain = domain

    @classmethod
    def from_serialised(cls, serialised):
        """load from pickle bytestring, as serialised by marshal()"""
        raw = pickle.loads(serialised)
        return cls(**raw)

    def parsed_results_result(self):
        return self._parse_results_result(self.results_result)

    @staticmethod
    def _parse_results_result(content):
        try:
            d = json.loads(content.decode("utf-8"))
            return d
        except Exception as exc:
            raise_from(CheckHostNetError("couldn't deserialise results_result from data %s" % content,), exc)

    def parsed_check_result(self):
        return self._parse_check_result(self.check_result)

    @staticmethod
    def _parse_check_result(content):
        try:
            d = json.loads(content.decode("utf-8"))
            return d
        except Exception as exc:
            raise_from(CheckHostNetError("couldn't deserialise check_result from data %s" % content,), exc)

    def __str__(self):
        try:
            return "domain: %s, check_result: %s, results_result: %s" % ( self.domain, self.check_result.decode("utf-8"), self.results_result.decode("utf-8") )
        except Exception as exc:
            log.warning(exc)
            return "domain: %s, contents could not be decoded" % (self.domain)

    def marshal(self):
        """return results as raw byte strings, encoded into a pickle byte string.
        using pickle here so as to avoid forcing a string encoding on the contents."""
        return pickle.dumps(
            obj={
                "domain": self.domain,
                "check_result": self.check_result,
                "results_result": self.results_result,
            },
            protocol=2
        )

    def __eq__(self, other):
        """perform a deep eq of our two attributes"""
        return (self.check_result == other.check_result) and (self.results_result == other.results_result)


class CheckHostNet(object):
    headers = {'Accept': 'application/json'}
    check_url_template = "https://check-host.net/check-dns?host=https://{domain}&max_nodes={max_nodes}"
    results_url_template = "https://check-host.net/check-result/{id}"

    def __init__(self):
        self.check_url_template = CheckHostNet.check_url_template
        self.results_url_template = CheckHostNet.results_url_template
        self.headers = CheckHostNet.headers

        self.session = requests.Session()

    def _get_or_error(self, url, headers):
        try:
            log.debug("requesting url %s" % url)
            res = self.session.get(url, headers=headers)
            res.raise_for_status()
            return res
        except Exception as exc:
            raise_from(CheckHostNetError("couldn't call check backend"), exc)

    def resolve_dns(self, domain, max_nodes=10):

        # parse check results, as we need them to call the real results
        # we won't save the output here
        check_res = self._get_or_error(
            url=self.check_url_template.format(domain=domain, max_nodes=max_nodes),
            headers=self.headers,
        )

        check_res_parsed = CheckHostNetResult._parse_check_result(check_res.content)

        results_res = self._get_or_error(
            url=self.results_url_template.format(id=check_res_parsed["request_id"]),
            headers=self.headers,
        )

        return CheckHostNetResult(
            domain=domain,
            check_result=check_res.content,
            results_result=results_res.content,
        )
