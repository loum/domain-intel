"""Analyst question and answers class :class:`domain_intel.analyst.Qas`
"""
import io
import json
from logga import log

import domain_intel
import domain_intel.utils


class Qas(domain_intel.Pipeline):
    """Problem domain for Analyst Questions and Answers.

    """
    def add_raw_qas(self,
                    filename,
                    max_add_count=None,
                    topic='analyst-qas',
                    dry=False):
        """Add the Analyst QA's defined in *filename* to the Kafka *topic*.

        *filename* is the name of the file source Excel file.

        *max_add_count* is a threshold that limits the number of
        QAs to publish.  If set to `None` then threshold is
        ignored and all available messages are published.

        *topic* defines the Kafka topic to publish to.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            number of QAs successfully added to the Kafka topic

        """
        artifacts_loaded = 0
        with self.producer() as producer:
            for artifact in domain_intel.utils.analyst_xls_to_json(filename,
                                                                   dry=True):
                if not dry:
                    producer.send(topic, artifact.rstrip().encode('utf-8'))
                    artifacts_loaded += 1

                if (max_add_count is not None and
                        (artifacts_loaded >= max_add_count)):
                    log.info('Analyst QAs threshold %d breached - exiting',
                             max_add_count)
                    break

        return artifacts_loaded

    def add_qas(self,
                filename,
                max_add_count=None,
                topic='analyst-qas',
                dry=False):
        """Add the JSON-based Analyst QA's defined in *filename* to the
        Kafka *topic*.

        *filename* is the name of the source JSON file.

        *max_add_count* is a threshold that limits the number of
        QAs to publish.  If set to `None` then threshold is
        ignored and all available messages are published.

        *topic* defines the Kafka topic to publish to.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            number of QAs successfully added to the Kafka topic

        """
        artifacts_loaded = 0
        with io.open(filename) as _fh:
            data = json.loads(_fh.read().rstrip())

        with self.producer() as producer:
            for domain, value in data.items():
                if not dry:
                    artifact = json.dumps({domain: value})
                    producer.send(topic, artifact.encode('utf-8'))
                    artifacts_loaded += 1

                if (max_add_count is not None and
                        (artifacts_loaded >= max_add_count)):
                    log.info('Analyst QAs threshold %d breached - exiting',
                             max_add_count)
                    break

        return artifacts_loaded

    def persist(self,
                max_read_count=None,
                topic='analyst-qas',
                group_id='default',
                dry=False):
        """Takes Analyst QA records and writes to the persistent store.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default consumer *topic* is ``analyst-qas``.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            tuple structure representing counts for the total number of
            records consumed and the number of domains successfully
            published to the Kafka topic

        """
        log.debug('Analyst QAs persist worker set to read %s messages',
                  max_read_count or 'all')
        log.debug('Analyst QAs persist worker timeout set to %d',
                  self.timeout)
        log.debug('Analyst QAs persist group_id %s', group_id)

        total_messages_read = 0
        edge_count = 0

        with self.consumer(topic, group_id=group_id) as consumer:
            for message in consumer:
                total_messages_read += 1

                data = json.loads(message.value.decode('utf-8'))
                for domain, value in data.items():
                    kwargs = {
                        '_key': domain,
                        'data': value
                    }
                    self.store.collection_insert('analyst-qas',
                                                 kwargs,
                                                 dry)

                    edge_kwargs = {
                        '_key': domain,
                        '_from': 'domain/{}'.format(domain),
                        '_to': 'analyst-qas/{}'.format(domain),
                    }
                    if self.store.edge_insert('marked', edge_kwargs, dry):
                        edge_count += 1

                if (max_read_count is not None and
                        total_messages_read >= max_read_count):
                    log.info('Max read threshold %d breached: exiting',
                             max_read_count)
                    break

        log.info('Analyst QAs read|edge put count %d|%d',
                 total_messages_read, edge_count)

        return (total_messages_read, edge_count)
