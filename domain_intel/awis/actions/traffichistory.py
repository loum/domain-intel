"""AWIS TrafficHistory class abstraction
:class:`domain_intel.awis.actions.TrafficHistory`
"""
import json
import multiprocessing
import collections
import lxml.etree
import xmljson
from logga import log

import domain_intel
import domain_intel.parser
import domain_intel.awisapi.actions

CONFIG = domain_intel.common.CONFIG


class TrafficHistory(domain_intel.Awis):
    """Problem domain for the AWIS TrafficHistory action that returns
    Alexa traffic rank detail.

    """
    def __init__(self):
        super(TrafficHistory, self).__init__()

        kwargs = {
            'access_id': CONFIG.get('awis')['access_key_id'],
            'secret_access_key': CONFIG.get('awis')['secret_access_key']
        }
        domain_intel.Awis.api = domain_intel.awisapi.actions.TrafficHistory(**kwargs)

    def slurp_traffic(self,
                      max_read_count=None,
                      topic='traffic-domains',
                      group_id='default',
                      dry=False):
        """Slurp TrafficHistory detail from Alexa based on *domain*
        and then publish the results to *producer*.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is `traffic-domains`.

        The default Kafka *group_id* name used is `default`.  However,
        we can force a re-read of the topic's messages by overriding
        *group_id* with a unique value.

        If the *dry* flag is set then only report, don't run.

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of domains successfully
            published to the Kafka topics

        """
        count_q = multiprocessing.Queue()

        target = self.slurp_traffic_worker
        args = (count_q, max_read_count, topic, group_id)
        kwargs = {'dry': dry}
        domain_intel.utils.threader(self.threads, target, *args, **kwargs)

        total_read_count = 0
        total_put_count = 0
        while not count_q.empty():
            counter = count_q.get()
            total_read_count += counter[0]
            total_put_count += counter[1]

        log.info('TrafficHistory read|put count %d|%d',
                 total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def slurp_traffic_worker(self,
                             queue,
                             max_read_count,
                             topic,
                             group_id,
                             dry=False):
        """Slurp TrafficHistory worker.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The remaining parameter list is as per :meth:`slurp_traffic`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        total_messages_read = 0
        total_messages_put = 0

        with self.producer() as producer:
            with self.consumer(topic, group_id=group_id) as consumer:
                for message in consumer:
                    domain = message.value.decode('utf-8')

                    total_messages_read += 1
                    if not dry:
                        result = self.api.traffic_history(domain=domain)
                        if result is not None:
                            producer.send('alexa-traffic-results', result)
                            total_messages_put += 1

                    if (max_read_count is not None and
                            (total_messages_read >= max_read_count)):
                        log.info('Maximum read threshold %d breached - exiting',
                                 max_read_count)
                        break

        log.info('TrafficHistory worker read|put count %d|%d',
                 total_messages_read, total_messages_put)

        queue.put(tuple([total_messages_read, total_messages_put]))

    @staticmethod
    def flatten_xml(xml):
        """Batched Alexa responses need to be parsed and extracted into
        individual domain components ready for next data flow path.

        Also want to strip off redundant Alexa control XML elements
        that have no value in our problem domain.

        Args:
            *xml*: the source XML to process

        Returns:
            flattened JSON equivalent of the source *xml*

        """
        root = lxml.etree.fromstring(xml)

        xml_to_json = None
        if TrafficHistory._check_xml_response(root):
            xml_to_json = TrafficHistory._parse_xml(root)

        return xml_to_json

    @staticmethod
    def _parse_xml(root):
        """Take the lxml.Element *root* and extract the TrafficHistory
        detail from the source XML.

        Returns:
            flattened JSON variant of the source XML

        """
        xpath = '//a:TrafficHistoryResponse/b:Response/b:TrafficHistoryResult'
        _ns = domain_intel.common.NS
        results = root.xpath(xpath, namespaces=_ns)

        # See if we can find a DataUrl element to display.
        url_xpath = './b:Alexa/b:TrafficHistory/b:Site/text()'
        urls = [x.xpath(url_xpath, namespaces=_ns)[0] for x in results]
        log.info('TrafficHistory flattening domain: %s',
                 ', '.join(['"{}"'.format(x) for x in  urls]))

        # Extract the historical data.
        data_xpath = './b:Alexa/b:TrafficHistory'
        traffic = results[0].xpath(data_xpath, namespaces=_ns)

        bf_json = xmljson.BadgerFish(dict_type=collections.OrderedDict)
        ns_replace = r'{{{0}}}'.format(domain_intel.common.NS_20050711)
        xml_to_json = json.dumps(bf_json.data(traffic[0]))

        return  xml_to_json.replace(ns_replace, '')

    @staticmethod
    def _check_xml_response(root):
        """Verify that a valid response has been received in the
        *root* :class:`lxml.Element`.

        Returns:
            Boolean ``True`` on success.  ``False`` otherwise

        """
        xpath = '//a:TrafficHistoryResponse/a:Response/a:ResponseStatus'
        _ns = domain_intel.common.NS
        response_status = root.xpath(xpath, namespaces=_ns)
        if not response_status:
            xpath = '//a:TrafficHistoryResponse/b:Response/a:ResponseStatus'
            response_status = root.xpath(xpath, namespaces=_ns)

        response_xpath = './a:StatusCode/text()'
        response = [x.xpath(response_xpath, namespaces=_ns)[0] for x in response_status]

        if response[0] != 'Success':
            log.error('TrafficHistory XML reponse error ')

        return response[0] == 'Success'

    def flatten(self,
                max_read_count=None,
                topic='alexa-traffic-results',
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

        total_read_count = 0
        total_put_count = 0
        while not count_q.empty():
            counter = count_q.get()
            total_read_count += counter[0]
            total_put_count += counter[1]

        log.info('TrafficHistory flatten read|put count %d|%d',
                 total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def flatten_worker(self,
                       queue,
                       max_read_count,
                       topic,
                       group_id,
                       dry=False):
        """Read all Alexa TrafficHistory results from the Kafka *topic*.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        The parameter list is as per :meth:`flatten_worker`.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('TrafficHistory flatten worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('TrafficHistory flatten worker timeout set to %d',
                  self.timeout)

        total_messages_read = 0
        total_messages_put = 0

        with self.producer() as producer:
            with self.consumer(topic, group_id=group_id) as consumer:
                records_read = 0
                for message in consumer:
                    total_messages_read += 1
                    traffic = TrafficHistory.flatten_xml(message.value)
                    if traffic is None:
                        continue

                    if not dry:
                        total_messages_put += 1
                        producer.send('alexa-traffic-flattened',
                                      traffic.encode('utf-8'))

                    if (max_read_count is not None and
                            (records_read >= max_read_count)):
                        break

        log.info('TrafficHistory flatten worker read|put count %d|%d',
                 total_messages_read, total_messages_put)

        queue.put(tuple([total_messages_read, total_messages_put]))

    def persist(self,
                max_read_count=None,
                topic='alexa-traffic-flattened',
                group_id='default',
                dry=False):
        """Takes Alexa TrafficHistory records and writes to the persistent
        store.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is ``alexa-traffic-flattened``.

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

        log.debug('TrafficHistory persist worker read|put count %d|%d',
                  total_read_count, total_put_count)
        read_put_counts = (total_read_count, total_put_count)

        return read_put_counts

    def persist_worker(self, queue, max_read_count, topic, group_id, dry):
        """TrafficHistory persistent store worker.

        As this is a worker that could be part of a set of executing
        threads, the number of messages read is pushed onto the
        :class:`multiprocessing.Queue` *queue*.

        Returns:
            updated :class:`multiprocessing.Queue` *queue* instance
            with number of records processed

        """
        log.debug('TrafficHistory persist worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('TrafficHistory persist worker timeout set to %d',
                  self.timeout)
        log.debug('TrafficHistory persist group_id %s', group_id)

        total_messages_read = 0
        edge_count = 0

        with self.consumer(topic, group_id=group_id) as consumer:
            for message in consumer:
                total_messages_read += 1

                data = json.loads(message.value.decode('utf-8'))
                parser = domain_intel.parser.TrafficHistory(data)
                self.store.collection_insert('traffic',
                                             parser.db_traffichistory_raw(),
                                             dry)

                if self.store.edge_insert('visit',
                                          parser.db_visit_edge(),
                                          dry):
                    edge_count += 1

                if (max_read_count is not None and
                        total_messages_read >= max_read_count):
                    log.info('Max read threshold %d breached - exiting',
                             max_read_count)
                    break

            log.info('TrafficHistory persist worker messages read %d',
                     total_messages_read)

        queue.put((total_messages_read, edge_count))
