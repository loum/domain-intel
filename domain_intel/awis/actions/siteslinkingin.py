"""AWIS SitesLinkingIn class abstraction
:class:`domain_intel.awis.actions.SitesLinkingIn`
"""
import json
import hashlib
import multiprocessing
from logga import log

import domain_intel
import domain_intel.awisapi.actions
import domain_intel.common

CONFIG = domain_intel.common.CONFIG
SLI_COUNT = 20
MAX_SLURPS = 10000


class SitesLinkingIn(domain_intel.Awis):
    """Problem domain for the AWIS SitesLinkingIn action that returns
    identifies sites linking into a given domain.

    """
    def __init__(self):
        super(SitesLinkingIn, self).__init__()

        kwargs = {
            'access_id': CONFIG.get('awis')['access_key_id'],
            'secret_access_key': CONFIG.get('awis')['secret_access_key']
        }
        domain_intel.Awis.api = domain_intel.awisapi.actions.SitesLinkingIn(**kwargs)

    def slurp_sites_linking_in(self,
                               domain,
                               max_slurps=None,
                               as_json=False,
                               dry=False):
        """Get list of sites linking into *domain*.

        Alexa places an upper limit of 20 on the number of sites that it
        will return per request (or a "slurp".  Subsequent calls must be
        made by incrementing the `Start` request parameter to indicate the
        page to return.  Since there is not way to know how many pages
        need to be slurped, we must test the current result for a list of
        titles.  If no titles are returned or *max_slurps* is breached
        (which ever comes first) then we exit.

        Returns:
            list of titles slurped.  If *as_json* is set then the resultant
            set is returned as a JSON structure

        """
        if max_slurps is None:
            max_slurps = MAX_SLURPS

        all_titles = []
        for start_index in range(max_slurps):
            if start_index >= max_slurps:
                log.debug('SitesLinkingIn domain "%s" threshold breached',
                          domain)
                break
            log.debug('SitesLinkingIn domain "%s" slurp iteration %d of %d',
                      domain, start_index + 1, max_slurps)

            response = None
            if not dry:
                response = self.api.sites_linking_in(domain,
                                                     start_index*SLI_COUNT)
            parser = domain_intel.awisapi.parser.SitesLinkingIn(response)
            titles = parser.extract_titles()

            if titles:
                all_titles.extend(titles)
            else:
                log.info('SitesLinkingIn slurp iteration %d returned '
                         'zero titles: exiting', start_index + 1)
                break

        unique_titles = SitesLinkingIn.unique_titles(all_titles)
        if as_json:
            unique_titles = json.dumps(unique_titles,
                                       sort_keys=True,
                                       indent=4)

        return unique_titles

    @staticmethod
    def unique_titles(titles):
        """Remove duplicate titles from the SitesLinkingIn results set.

        """
        reduced_titles = []
        cache = []

        for title in titles:
            if title.get('title') not in cache:
                cache.append(title.get('title'))
                reduced_titles.append(title)

        return reduced_titles

    def slurp_sites(self,
                    max_read_count=None,
                    topic='sli-domains',
                    group_id='default',
                    dry=False):
        """Slurp SitesLinkingIn detail from Alexa based on *domain*
        and then publish the results to *producer*.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is `sli-domains`.

        The default Kafka *group_id* name used is `default`.  However,
        we can force a re-read of the topic's messages by overriding
        *group_id* with a unique value.

        If the *dry* flag is set then only report, don't run.

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of domains successfully
            published to the Kafka topics

        """
        total_messages_read = total_messages_put = 0

        while True:
            domain = self._get_message(topic, group_id)
            if not domain:
                break

            total_messages_read += 1
            results = self.slurp_sites_linking_in(domain=domain, dry=dry)
            if results:
                sites = {
                    'domain': domain,
                    'urls': results,
                }
                message = json.dumps(sites).encode('utf-8')
                if not dry:
                    with self.producer() as producer:
                        producer.send('alexa-sli-results', message)
                    total_messages_put += 1

            if (max_read_count is not None and
                    (total_messages_read >= max_read_count)):
                log.info('Maximum read threshold %d breached - exiting',
                         max_read_count)
                break

        log.info('SitesLinkingIn read|put count %d|%d',
                 total_messages_read, total_messages_put)

        return tuple([total_messages_read, total_messages_put])

    def _get_message(self, topic, group_id='default'):
        """Due to timeout issues, simply reads a single message from
        the Kafka *topic*.

        Returns:
            the next consumed message or `None`

        """
        with self.consumer(topic, group_id=group_id) as consumer:
            domain = None
            for message in consumer:
                domain = message.value.decode('utf-8')
                break

        return domain

    def parse_raw_siteslinkingin(self,
                                 file_h,
                                 max_read_count=None,
                                 topic='alexa-sli-results',
                                 dry=False):
        """Re-load raw Alexa SitesLinkingIn action (as JSON) back
        into the Kafka *topic*.

        Alexa SitesLinkingIn action (as JSON) structure is as per::

            {
                "domain": "allmp3s.xyz",
                "urls": [
                    {"title": ...}
                ]
            }

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of records successfully
            published to the Kafka topic

        """
        file_h = domain_intel.utils.standardise_file_handle(file_h)

        records_read = records_put = 0

        with self.producer() as producer:
            for raw_line in file_h:
                records_read += 1

                if not dry:
                    producer.send(topic,
                                  raw_line.rstrip().encode('utf-8'))
                    records_put += 1

                if (max_read_count is not None and
                        records_read >= max_read_count):
                    log.info('Maximum read threshold %d breached: exiting',
                             max_read_count)
                    break

        return tuple([records_read, records_put])

    def persist(self,
                max_read_count=None,
                topic='alexa-sli-results',
                group_id='default',
                dry=False):
        """Takes Alexa SitesLinkingIn records and writes to the persistent
        store.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is `alexa-sli-results`.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of domains successfully
            published to the Kafka topic

        """
        count_q = multiprocessing.Queue()

        target = self.persist_worker
        args = (count_q, max_read_count, topic, group_id)
        kwargs = {'dry': dry}
        domain_intel.utils.threader(self.threads, target, *args, **kwargs)

        total_read_count = 0
        total_put_count = 0
        while not count_q.empty():
            counter = count_q.get()
            total_read_count += counter[0]
            total_put_count += counter[1]

        log.debug('SitesLinkingIn read|put count %d|%d',
                  total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def persist_worker(self,
                       queue,
                       max_read_count,
                       topic,
                       group_id,
                       dry):
        """Write out the SitesLinkingIn information to a persistent store.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('SitesLinkingIn persist worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('Persist worker timeout set to %d', self.timeout)
        log.debug('Persist group_id %s', group_id)

        messages_read = edge_count = 0

        with self.consumer(topic, group_id=group_id) as consumer:
            for message in consumer:
                data = message.value.decode('utf-8')

                messages_read += 1
                edge_count += self.extract_siteslinkingin(data, dry=dry)

                if (max_read_count is not None and
                        messages_read >= max_read_count):
                    log.info('Max read threshold %d breached - exiting',
                             max_read_count)
                    break

            log.debug('SitesLinkingIn persist worker messages read %d',
                      messages_read)

        queue.put((messages_read, edge_count))

    def extract_siteslinkingin(self, data, dry):
        """Takes *data* and re-organises the structure so that it can
        be inserted into the Domain Intel data model's graph relationship.

        *dry* run will only simulate execution and not write to the
        persistent store.

        Returns:
            count of records inserted into the graph edge

        """
        edge_count = 0

        as_json = json.loads(data)
        domain = as_json.get('domain')
        urls = as_json.get('urls', [])

        for url in urls:
            domain_linkingin = url.get('title')
            page = url.get('url')
            signature = hashlib.md5()
            signature.update(page.encode('utf-8'))
            url_key = signature.hexdigest()
            url_kwargs = {
                '_key': url_key,
                'domain_linkingin': domain_linkingin,
            }
            self.store.collection_insert('url', url_kwargs, dry)

            links_into_kwargs = {
                '_key': '{}:{}'.format(domain, url_key),
                'label': url.get('url'),
                '_from': 'url/{}'.format(url_key),
                '_to': 'domain/{}'.format(domain),
            }
            if self.store.edge_insert('links_into', links_into_kwargs, dry):
                edge_count += 1

        return edge_count
