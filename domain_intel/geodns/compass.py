from __future__ import print_function, unicode_literals
from future.utils import raise_from

import requests
import time
from logga import log
import json


class CompassServerError(Exception):
    pass

class CompassServerEmptyResponse(Exception):
    pass


class CompassHTTPResolver(object):

    lookup_url_template = "https://{username}:{password}@api.ip-echelon.com/compass/verbose_lookup"

    def __init__(self, username, password):
        self.lookup_url_template = CompassHTTPResolver.lookup_url_template
        self.username = username
        self.password = password
        self.session = requests.Session()

    def url(self):
        return self.lookup_url_template.format(username=self.username, password=self.password)

    def resolve(self, ipv4, time_epoch=None):
        """resolve an ipv4 address and optional point in time to a geog record (see ipechelon.geog* in aurora.
        for in depth documentation, look at the compass project.
        
        this method does not have an 'unparsed' representation, as we control both ends."""
        url = self.url()
        log.debug("compass requesting %s", ipv4,)
        try:
            res = self.session.post(
                url,
                data=json.dumps({
                    "ip": ipv4,
                    "time": time_epoch if time_epoch else int(time.time()),
                })
            )

            response = self._parse_results(res.content)
            res.raise_for_status()
            return response

        # bubble this up to caller
        except CompassServerEmptyResponse as exc:
            raise exc
        # wrap generic fatal error
        except Exception as exc:
            raise_from(CompassServerError("couldn't call geog compass backend: %s" % (res.content)), exc)

    @staticmethod
    def _parse_results(content):
        try:
            d = json.loads(content.decode("utf-8"))
        except Exception as exc:
            raise_from(CompassServerError("couldn't deserialise geog compass backend response"), exc)

        # if error response, check for 'clean' error of no routes
        if "Error" in d and d["Error"] == "no routes":
            raise CompassServerEmptyResponse("compass server gave empty response")

        return d
