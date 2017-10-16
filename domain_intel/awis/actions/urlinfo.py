""":class:`UrlInfo`

"""
import sys
import json
import time
import csv
import tempfile
import multiprocessing
import collections
import hashlib
import lxml.etree
import xmljson
from logga import log

import domain_intel
import domain_intel.utils
import domain_intel.awisapi.actions
import domain_intel.awisapi.parser

CONFIG = domain_intel.common.CONFIG


class UrlInfo(domain_intel.Awis):
    """Problem domain from the AWIS UrlInfo action that returns
    Alexa domain detail.

    """
    def __init__(self):
        super(UrlInfo, self).__init__()

        kwargs = {
            'access_id': CONFIG.get('awis')['access_key_id'],
            'secret_access_key': CONFIG.get('awis')['secret_access_key']
        }
        domain_intel.Awis.api = domain_intel.awisapi.actions.UrlInfo(**kwargs)

    def add_domain_labels(self, max_add_count=None, dry=False):
        """Add the "domain" collection labels to a Kafka topic.

        *max_add_count* is a threshold that limits the number of
        domains to publish.  If set to `None` then threshold is
        ignored and all available messages are published.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            number of domains successfully added to the Kafka topic

        """
        labels_loaded = 0

        with self.producer() as producer:
            for index, domain in enumerate(self.store.export_ids('domain'), 1):
                if not dry:
                    producer.send('domain-labels',
                                  domain.rstrip().encode('utf-8'))
                    labels_loaded += 1
                log.info('Domain "%s" added', domain.rstrip())
                if index % 1000 == 0:
                    log.info('WIP labels added: %d', labels_loaded)

                if max_add_count is not None and index >= max_add_count:
                    log.info('Maximum threshold %d breached - exiting',
                             max_add_count)
                    break

        return labels_loaded

    def read_domains(self,
                     max_read_count=5,
                     topic='gtr-domains',
                     group_id='default',
                     slurp=False,
                     dry=False):
        """Simple domain dump method that will return *max_read_count*
        batches of domains from the *group_id* Kafka group.  Here,
        a batched domain is a 5-domain list.

        Will spawn 5 separate processes that will each read from a
        topic partition.

        The hardwired Kafka topic read from is `gtr-domains`.

        If *max_read_count* is `None` then all domains will be
        returned.

        The default Kafka *group_id* name used is `default`.  However,
        we can force a re-read of the topic's messages by overriding
        *group_id* with a unique value.

        Returns:
            total count of records read

        """
        count_q = multiprocessing.Queue()

        target = self.read_worker
        args = (count_q, max_read_count, topic, group_id, slurp)
        kwargs = {'dry': dry}
        domain_intel.utils.threader(self.threads, target, *args, **kwargs)

        total_count = 0
        while not count_q.empty():
            total_count += count_q.get()

        return total_count

    def read_worker(self,
                    queue,
                    max_read_count,
                    topic,
                    group_id,
                    slurp,
                    dry=False):
        """Read all domains from the Kafka partitions.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The parameter list is as per :meth:`read_domains`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('Read worker set to read %s messages',
                  max_read_count or 'all')

        with self.producer() as producer:
            with self.consumer(topic, group_id) as consumer:
                domain_batch = []
                messages_read = 0
                total_messages_read = 0

                for message in consumer:
                    messages_read += 1
                    total_messages_read += 1

                    domain_batch.append(message.value.rstrip())
                    if (len(domain_batch) > 4 or
                            (max_read_count is not None and
                             messages_read >= max_read_count)):
                        if slurp:
                            self.slurp_domains(producer, domain_batch, dry=dry)
                        else:
                            log.info('Domains pending: %s', domain_batch)
                        del domain_batch[:]
                        messages_read = 0

                    if (max_read_count is not None and
                            (total_messages_read >= max_read_count)):
                        break

                # ... and check for laggards.
                if domain_batch:
                    log.info('Processing laggards before close')
                    if slurp:
                        self.slurp_domains(producer, domain_batch, dry=dry)
                    else:
                        log.info('Domains pending: %s', domain_batch)

        queue.put(total_messages_read)

    def slurp_domains(self, producer, domains, dry=False):
        """Get domain information from Alexa.

        Args:
            *producer*: :class:`kafka.producer.KafkaProducer` instance

            *domains*: either a string value representing the name of the
            domain to search or a list of domain names.

            *dry*: only report, don't run

        """
        results = self.api.url_info(domains)

        if not dry:
            producer.send('alexa-results', results.rstrip())

    def flatten_domains(self,
                        max_read_count=None,
                        topic='alexa-results',
                        group_id='default',
                        dry=False):
        """Takes the Alexa batched domain results and
        split out into separate, JSON equivalents.

        Kwargs:
            *max_read_count*: number of batched domains to read.  `None`
            returned all offsets associated with the *group_id*

            *group_id*: Kafka managed consumer element that manages
            the messages read from the topic

        """
        count_q = multiprocessing.Queue()

        target = self.flatten_worker
        args = (count_q, max_read_count, topic, group_id)
        kwargs = {'dry': dry}
        domain_intel.utils.threader(self.threads, target, *args, **kwargs)

        total_count = 0
        while not count_q.empty():
            total_count += count_q.get()

        log.debug('Flatten workers total records read %d', total_count)

        return total_count

    def flatten_worker(self,
                       queue,
                       max_read_count,
                       topic,
                       group_id,
                       dry=False):
        """Read all Alexa results from the Kafka partitions.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The parameter list is as per :meth:`flatten_domains`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('UrlInfo flatten worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('UrlInfo flatten worker timeout set to %d', self.timeout)

        with self.producer() as producer:
            with self.consumer(topic, group_id=group_id) as consumer:
                records_read = 0
                for message in consumer:
                    records_read += 1

                    for domain in UrlInfo.flatten_batched_xml(message.value):
                        if not dry:
                            producer.send('alexa-flattened',
                                          domain.encode('utf-8'))

                    if (max_read_count is not None and
                            (records_read >= max_read_count)):
                        break

        log.debug('UrlInfo flatten worker records read %d', records_read)

        queue.put(records_read)

    @staticmethod
    def flatten_batched_xml(xml):
        """Batched Alexa responses need to be parsed and extracted into
        individual domain components ready for next data flow path.

        Also want to strip off redundant Alexa control XML elements
        that have no value in our problem domain.

        Args:
            *xml*: the source XML to process

        Returns:
            list of domain-based XML

        """
        root = lxml.etree.fromstring(xml)
        xpath = '//a:UrlInfoResponse/b:Response/b:UrlInfoResult'
        _ns = domain_intel.common.NS
        xml_domains = root.xpath(xpath, namespaces=_ns)

        # See if we can find a DataUrl element to display.
        url_xpath = './b:Alexa/b:ContentData/b:DataUrl/text()'
        urls = [x.xpath(url_xpath, namespaces=_ns)[0] for x in xml_domains]
        log.info('Batched URLs sourced: %s',
                 ', '.join(['"{}"'.format(x) for x in  urls]))

        bf_json = xmljson.BadgerFish(dict_type=collections.OrderedDict)
        ns_replace = r'{{{0}}}'.format(domain_intel.common.NS_20050711)
        xml_to_json = [json.dumps(bf_json.data(x)) for x in xml_domains]

        return [x.replace(ns_replace, '') for x in xml_to_json]

    def persist(self,
                max_read_count=None,
                topic='alexa-flattened',
                group_id='default',
                dry=False):
        """Persist flattened (processed) Alexa domain data to ArangoDB
        executor.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is `alexa-flattened`.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            total count of records written to the DB across all workers

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

        log.debug('UrlInfo persist worker read|put count %d|%d',
                  total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def persist_worker(self,
                       queue,
                       max_read_count,
                       topic,
                       group_id,
                       dry=False):
        """Persist flattened (processed) Alexa domain data to ArangoDB
        worker.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The parameter list is as per :meth:`persist`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('Data persist worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('Persist worker timeout set to %d', self.timeout)

        total_messages_read = 0
        put_count = 0

        with self.consumer(topic, group_id) as consumer:
            for message in consumer:
                total_messages_read += 1

                self.write_to_store(message.value, dry)
                # TODO: quantify successful insert.
                put_count += 1

                if (max_read_count is not None and
                        total_messages_read >= max_read_count):
                    log.info('Maximum read threshold %d breached - exiting',
                             max_read_count)
                    break

        log.info('UrlInfo persist worker messages read %d',
                 total_messages_read)

        queue.put((total_messages_read, put_count))

    def write_to_store(self, message, dry):
        """Entry point for the Domain Intel data model abstraction that will
        transpose a *message* into the persistent store managed by
        attr:`database`.

        *message* is a :class:`kafka.consumer.fetcher.ConsumerRecord`
        instance that contains the actual message value consumed from the
        target Kafka topic.

        The *dry* flag will simulate execution.  No records will be created.

        """
        url_info = domain_intel.awisapi.parser.UrlInfo(message)

        self.persist_url_info(url_info, dry)
        self.persist_domain(url_info, dry)
        self.persist_country_rank(url_info, dry)
        self.persist_related_links(url_info, dry)
        self.persist_contributing_subdomains(url_info, dry)

    def persist_url_info(self, url_info, dry=False):
        """Persist the Alexa URL info raw JSON.

        *url_info* is the parsed
        :class:`domain_intel.awisapi.parser.UrlInfo` instance that is
        used to build the collection documents.

        """
        kwargs = {
            '_key': url_info.domain,
            'data': url_info.raw,
        }
        self.store.collection_insert('url-info', kwargs, dry)

    def persist_domain(self, url_info, dry=False):
        """Persist the Alexa domain rank per country.

        *url_info* is the parsed
        :class:`domain_intel.awisapi.parser.UrlInfo` instance that is
        used to build the collection documents.

        """
        kwargs = url_info()
        kwargs.update({'_key': url_info.domain})
        self.store.collection_insert('domain', kwargs, dry)

    def persist_country_rank(self, url_info, dry=False):
        """Persist the Alexa domain rank per country.

        *url_info* is the parsed
        :class:`domain_intel.awisapi.parser.UrlInfo` instance that is
        used to build the collection documents.

        Returns:
            number of edge collections inserted into the graph

        """
        edge_count = 0
        for country_code, rank in url_info.domain_rank_by_country:
            ranked_kwargs = {
                '_key': '{}:{}'.format(url_info.domain, country_code),
                '_from': 'domain/{}'.format(url_info.domain),
                '_to': 'country/{}'.format(country_code),
                'label': rank
            }
            if self.store.edge_insert('ranked', ranked_kwargs, dry):
                edge_count += 1

        return edge_count

    def persist_related_links(self, url_info, dry=False):
        """Persist the Alexa related links.

        *graph* is a :class:`arango.graph.Graph` instance that can be
        used to extract the vertex and edge collections associated
        with the link.  *url_info* is the parsed
        :class:`domain_intel.awisapi.parser.UrlInfo` instance that is
        used to build the collection documents.

        Returns:
            number of edge collections inserted into the graph

        """
        edge_count = 0
        for url, nav_url, title in url_info.related_links:
            signature = hashlib.md5()
            signature.update(url.encode('utf-8'))
            url_key = signature.hexdigest()[:16]
            link_kwargs = {
                '_key': url_key,
                'navigable_url': nav_url,
                'title': title,
            }
            self.store.collection_insert('link', link_kwargs, dry)

            related_kwargs = {
                '_key': '{}:{}'.format(url_info.domain, url_key),
                'label': url,
                '_from': 'domain/{}'.format(url_info.domain),
                '_to': 'link/{}'.format(url_key),
            }
            if self.store.edge_insert('related', related_kwargs, dry):
                edge_count += 1

        return edge_count

    def persist_contributing_subdomains(self, url_info, dry=False):
        """Persist the Alexa contributing subdomains.

        *url_info* is the parsed
        :class:`domain_intel.awisapi.parser.UrlInfo` instance that is
        used to build the collection documents.

        Returns:
            number of edge collections inserted into the graph

        """
        edge_count = 0
        for subdomain in url_info.contributing_subdomains:
            contribute_kwargs = {
                '_key': subdomain[0],
                'months': subdomain[1],
                'reach_pc': subdomain[2],
                'page_views_pc': subdomain[3],
                'page_views_per_user': subdomain[4],
            }
            self.store.collection_insert('subdomain',
                                         contribute_kwargs,
                                         dry)

            related_kwargs = {
                '_key': '{}:{}'.format(url_info.domain, subdomain[0]),
                '_from': 'subdomain/{}'.format(subdomain[0]),
                '_to': 'domain/{}'.format(url_info.domain),
            }
            if self.store.edge_insert('contribute', related_kwargs, dry):
                edge_count += 1

        return edge_count

    def traverse_relationship(self,
                              max_read_count=None,
                              topic='domain-labels',
                              group_id='default',
                              dry=False):
        """Read domain labels from the Kafka topic *topic*
        and uses that that the starting vertex to traverse the graph.
        The hardwired Kafka topic read from is `domain-labels`.

        If *max_read_count* is `None` then all domains will be
        returned.

        The default Kafka *group_id* name used is `default`.  However,
        we can force a re-read of the topic's messages by overriding
        *group_id* with a unique value.

        Returns:
            total count of records read

        """
        log.debug('Traverse worker set to read %s messages',
                  max_read_count or 'all')

        with self.producer() as producer:
            with self.consumer(topic, group_id) as consumer:
                total_messages_read = 0

                for message in consumer:
                    label = message.value.decode('utf-8')
                    result = self.store.traverse_graph(label)
                    if result is None:
                        continue

                    total_messages_read += 1
                    if not dry:
                        producer.send('domain-traversals',
                                      result.encode('utf-8'))

                    if (max_read_count is not None and
                            (total_messages_read >= max_read_count)):
                        break

        log.debug('Domains traverser worker records read %d',
                  total_messages_read)

        return total_messages_read

    def wide_column_dump(self,
                         max_read_count=None,
                         topic='domain-traversals',
                         group_id='default',
                         dry=False):
        """Takes Domain Intel graph data and dumps to a wide-column CSV
        format suitable for ingest into Google BigQuery

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is `domain-traversal`.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of domains successfully
            published to the Kafka topic

        """
        count_q = multiprocessing.Queue()

        target = self.wide_column_dump_worker
        args = (count_q, max_read_count, topic, group_id)
        kwargs = {'dry': dry}
        domain_intel.utils.threader(self.threads, target, *args, **kwargs)

        total_read_count = 0
        total_put_count = 0
        while not count_q.empty():
            counter = count_q.get()
            total_read_count += counter[0]
            total_put_count += counter[1]

        log.debug('Wide-column CSV dump read|put count %d|%d',
                  total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def wide_column_dump_worker(self,
                                queue,
                                max_read_count,
                                topic,
                                group_id,
                                dry):
        """Wide-column CSV dump worker.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The parameter list is as per :meth:`wide_column_dump`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('Wide-column CSV dump worker set to read %s messages',
                  max_read_count or 'all')

        with self.producer() as producer:
            with self.consumer(topic, group_id=group_id) as consumer:
                total_messages_read = 0
                total_messages_put = 0
                for message in consumer:
                    traversal = json.loads(message.value.decode('utf-8'))
                    reporter = domain_intel.Reporter(data=traversal)
                    total_messages_read += 1
                    for line in reporter.dump_wide_column_csv():
                        if not dry:
                            producer.send('wide-column-csv',
                                          line.encode('utf-8'))
                        total_messages_put += 1

                    if (max_read_count is not None and
                            (total_messages_read >= max_read_count)):
                        break

        queue.put((total_messages_read, total_messages_put))

    def alexa_csv_dump(self,
                       max_read_count=None,
                       topic='alexa-flattened',
                       group_id='custom',
                       dry=False):
        """Simple CSV dump of targetted Alexa data.

        This method skips the read from the persistent store and
        simply reads from the flattened Alexa Kafka topic.  These messages
        present as JSON.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The *dry* flag will simulate execution.  No output CSV will be
        created.

        Returns:
            number of messages read

        """
        log.debug('Alexa dump worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('Alexa dump worker timeout set to %d', self.timeout)

        # Make sure we read files as unicode for both python 2 and 3.
        if sys.version_info.major >= 3:
            rank_csv = tempfile.NamedTemporaryFile(mode='w', delete=dry)
            country_rank_csv = tempfile.NamedTemporaryFile(mode='w',
                                                           delete=dry)
        else:
            rank_csv = tempfile.NamedTemporaryFile(delete=dry)
            country_rank_csv = tempfile.NamedTemporaryFile(delete=dry)

        rank_writer = csv.writer(rank_csv)
        country_rank_writer = csv.writer(country_rank_csv)

        with self.consumer(topic, group_id) as consumer:
            messages_read = 0
            for message in consumer:
                messages_read += 1
                flattened_alexa = message.value.decode('utf-8')
                stats = UrlInfo.alexa_flattened_extract(flattened_alexa)
                rank_writer.writerow(stats[0])
                if stats[1]:
                    country_rank_writer.writerows(stats[1])

                if messages_read % 10000 == 0:
                    log.info('Exported %d domains to CSV', messages_read)

                if (max_read_count is not None and
                        messages_read >= max_read_count):
                    log.info('Maximum read threshold %d breached - exiting',
                             max_read_count)
                    break

        log.info('Global rank file %s', rank_csv.name)
        log.info('Country rank file %s', country_rank_csv.name)
        log.info('Alexa dump worker domains read %d', messages_read)

        rank_csv.close()
        country_rank_csv.close()

        return messages_read

    @staticmethod
    def alexa_flattened_extract(alexa_json):
        """Rules to extract fields from a given *alexa_json* record.

        Extraction takes out:
        * domain rank
        * domain rank by country

        *alexa_json* should be a flattened Alexa record in JSON format.

        Returns:
            A tuple structure of the form::

                (
                    [
                        'watchseriesonline.io',
                        1490187600.0,
                        588080,
                    ],
                    [
                        [
                            'abc.com',
                            1490187600.0,
                            'VE',
                            29418
                        ],
                        ...
                    ]
                )

        """
        epoch = time.time()
        alexa_data = json.loads(alexa_json)
        base = alexa_data['UrlInfoResult']['Alexa']

        traffic_data = base.get('TrafficData')
        data_url = traffic_data.get('DataUrl').get('$')
        log.info('Exporting domain "%s"', data_url)
        rank = traffic_data.get('Rank').get('$')
        rank_stats = [data_url, epoch, rank]

        country_ranks = []
        rank_by_country = traffic_data.get('RankByCountry').get('Country')

        if rank_by_country is not None:
            if not isinstance(rank_by_country, list):
                rank_by_country = [rank_by_country]

            for country in rank_by_country:
                code = country.get('@Code')
                if code == 'O':
                    continue
                rank = country.get('Rank').get('$')
                country_values = [data_url, epoch, code, rank]

                country_ranks.append(country_values)

        return tuple([rank_stats, country_ranks])
