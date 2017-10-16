""":class:`AwisApi`

"""
import base64
import datetime
import hashlib
import hmac
from logga import log
from future.moves.urllib.parse import urlencode, urlunparse
from future.moves.urllib.request import urlopen
from future.moves.urllib.error import HTTPError

HTTP_METHOD = 'GET'
AWIS_HOST = 'awis.amazonaws.com'
PATH = '/'
SIGNATURE_VERSION = 2
SIGNATURE_METHOD = 'HmacSHA1'
MAX_BATCH_REQUESTS = 5
MAX_SITES_LINKING_IN_COUNT = 20
MAX_CATEGORY_LISTINGS_COUNT = 20
DATE_FMT = '%Y-%m-%dT%H:%M:%S.000Z'
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


class AwisApi(object):
    """Alexa Web Information Service API abstraction.

    Sample usage::

        import domain_intel
        api = domain_intel.AwisApi(<access_id>, <secret_access_key>)
        api.url_info('google.com.au', ['Rank', 'LinksInCount'])
        '<?xml version="1.0"?>\n<aws:UrlInfoResponse
        ...
        <aws:StatusCode>Success</aws:StatusCode>...'

    """
    def __init__(self, access_id, secret_access_key):
        self.__access_id = access_id
        self.__secret_access_key = secret_access_key

    def calculate_signature(self, params):
        """Calculate the AWS Signature v2 as per
        `these notes <https://goo.gl/DR0sqs>`_.

        **Args:**
            *query_string*: as per the return value of
            :meth:`canonicalized_query_string`

        **Returns:**
            the signed *query_string*

        """
        pre_signed = '\n'.join([HTTP_METHOD,
                                AWIS_HOST,
                                PATH,
                                self.canonicalized_query_string(params)])
        hmac_signature = hmac.new(self.__secret_access_key.encode('utf-8'),
                                  pre_signed.encode('utf-8'),
                                  hashlib.sha1)
        signature = base64.b64encode(hmac_signature.digest())

        return signature

    def default_params(self):
        """The default parameter section of the AWIS HTTP request.

        """
        return {
            'AWSAccessKeyId': self.__access_id,
            'SignatureMethod': SIGNATURE_METHOD,
            'SignatureVersion': SIGNATURE_VERSION,
            'Timestamp':
                datetime.datetime.utcnow().strftime(DATE_FMT)
        }

    def build_url(self, params):
        """Build HTTP request to send to AWIS.

        **Args:**
            *params*:

        **Returns:**
            the complete URL that can be fed into the AWIS API

        """
        params.update(self.default_params())
        params.update({'Signature': self.calculate_signature(params)})

        url_components = [
            'http',
            AWIS_HOST,
            PATH,
            None,
            self.canonicalized_query_string(params),
            None,
        ]

        url = urlunparse(url_components)

        return url

    @staticmethod
    def request(url, tries=3):
        """Wrapper around :func:`urlopen` to AWIS call.

        On failure, will attempt another 2 tries for success.

        **Args:**
            *url*: the AWIS URL to call

            *tries*: number of failed tries allowed before flagging this
            attempt as a failure

        **Returns:**
            the HTTP response value

        """
        failed_requests = 0
        response_value = None
        while failed_requests < tries:
            try:
                log.debug('Request %d of %d: "%s"',
                          (failed_requests + 1), tries, url)
                response = urlopen(url)
                if response.code == 200:
                    response_value = response.read()
                    break
            except HTTPError as err:
                log.error('Request failed "%s"', err)

            failed_requests += 1
            if failed_requests >= tries:
                log.error('All requests failed')

        return response_value

    @staticmethod
    def canonicalized_query_string(params):
        """See `this guide <https://goo.gl/u2AzO7>`_ this guide for
        further details.

        Most can be accomplished with the :func:`urllib.urlencode` but we
        need to perform a sort pre-step.

        """
        tmp_params = [(key, params[key]) for key in sorted(params.keys())]

        return urlencode(tmp_params)
