"""AWIS slurping base class :class:`domain_intel.awis.Awis`
"""
import sys
import io
import domain_intel
import domain_intel.utils

from logga import log


class Awis(domain_intel.Pipeline):
    """Base class for Alexa actions.

    """
    def add_domains(self,
                    file_h,
                    max_add_count=None,
                    topic='gtr-domains',
                    dry=False):
        """Add the domains listed in *csv_file* to the Kafka topic.

        *file_h* is the file object reference to the source file.

        *max_add_count* is a threshold that limits the number of
        domains to publish.  If set to `None` then threshold is
        ignored and all available messages are published.

        *topic* defines the Kafka topic to publish to.

        The *dry* flag will simulate execution.  No records will be
        published.

        Returns:
            number of domains successfully added to the Kafka topic

        """
        # Make sure we read files as unicode for both python 2 and 3.
        filename = file_h.name
        file_h.close()
        file_h = io.open(filename, encoding='utf-8')

        lines_in_file = domain_intel.utils.get_file_line_count(file_h)
        domains_loaded = 0
        with self.producer() as producer:
            for index, domain in enumerate(file_h, 1):
                if not dry:
                    producer.send(topic,
                                  domain.rstrip().encode('utf-8'))
                    domains_loaded += 1
                log.info('Domain "%s" added to topic "%s"',
                         domain.rstrip(), topic)
                if index % 1000 == 0:
                    log.info('Added %d of %d domains', index, lines_in_file)

                if max_add_count is not None and index >= max_add_count:
                    log.info('Maximum threshold %d breached - exiting',
                             max_add_count)
                    break

        return domains_loaded

    def parse_raw_alexa(self,
                        file_h,
                        topic,
                        end_token,
                        max_read_count=None,
                        dry=False):
        """Load raw Alexa XML back into the Kafka topic for
        re-processing.

        A typical Alexa XML reponse starts with the token::

            <?xml version="1.0"?>

        and ends with the token::

            </aws:<end_token>>

        Args:
            *file_h*: file object to the source file

        Returns:
            a tuple representing the read metrics in the form::

                (<lines_consumed>, <records_read>, <records_put))

        """
        file_h = domain_intel.utils.standardise_file_handle(file_h)

        start_token_processed = end_token_processed = False
        records_read = records_put = lines_consumed = 0
        xml_segment = []
        with self.producer() as producer:
            for raw_line in file_h:
                line = raw_line.rstrip()
                if r'<?xml version="1.0"?>' in line:
                    start_token_processed = True
                elif r'</aws:{}>'.format(end_token) in line:
                    end_token_processed = True

                if start_token_processed:
                    lines_consumed += 1
                    xml_segment.append(line)

                if start_token_processed and end_token_processed:
                    records_read += 1
                    if not dry:
                        message = ('\n'.join(xml_segment)).encode('utf-8')
                        producer.send(topic, message)
                        records_put += 1
                    start_token_processed = end_token_processed = False
                    del xml_segment[:]

                if (max_read_count is not None and
                        records_read >= max_read_count):
                    log.info('Maximum read threshold %d breached - exiting',
                             max_read_count)
                    break

        log.info('Lines read|XML records in|XML records put: %d|%d|%d',
                 lines_consumed, records_read, records_put)

        return tuple([lines_consumed, records_read, records_put])
