"""Domain Intel pipeline class :class:`domain_intel.Pipeline`
"""
import sys
import io
import domain_intel.utils
import domain_intel.common

from logga import log

CONFIG = domain_intel.common.CONFIG


class Pipeline(object):
    """
    .. attribute:: timeout
        number of milliseconds to block during message iteration before
        exiting

    .. attribute:: threads
        number of workers to spawn to leverage parallelisation

    .. attribute:: store
        reference to the persistent store

    """
    def __init__(self):
        kafka_conf = CONFIG.get('kafka')
        self.__bs_servers = kafka_conf.get('bootstrap_servers')
        self.__timeout = int(kafka_conf.get('timeout', 10000))
        self.__threads = CONFIG.get('threads')
        self.__api = None
        self.__store = None

    @property
    def bs_servers(self):
        """Bootstrap servers to Kafka.
        """
        return self.__bs_servers

    @property
    def timeout(self):
        """Number of seconds for the consumer to block before exiting.
        """
        return self.__timeout

    @property
    def threads(self):
        """Number of processing threads.
        """
        return self.__threads

    @property
    def api(self):
        """Reference to the :class:`domain_intel.alexaapi.actions.UrlInfo`
        """
        return self.__api

    @property
    def store(self):
        """Handle to the persistent store.
        """
        if self.__store is None:
            self.__store = domain_intel.Store()

        return self.__store

    def producer(self):
        """Wrapper around the :func:`domain_intel.utils.safe_producer`
        with default parameters.

        """
        kwargs = {'bootstrap_servers': self.bs_servers}
        return domain_intel.utils.safe_producer(**kwargs)

    def consumer(self, topic, group_id='default'):
        """Wrapper around the :func:`domain_intel.utils.safe_consumer`
        with default parameters.

        """
        kwargs = {
            'bootstrap_servers': self.bs_servers,
            'group_id': group_id,
            'consumer_timeout_ms': self.timeout,
        }
        return domain_intel.utils.safe_consumer(topic, **kwargs)

    def topic_dump(self,
                   max_read_count=None,
                   topic='wide-column-csv',
                   group_id='default',
                   dry=False):
        """Simple dump of messages from *topic*.

        *max_read_count* can limit the number of records read from *topic*.
        The default action is to read all available messages.

        The default Kafka *group_id* name used is `default`.  However,
        we can force a re-read of the topic's messages by overriding
        *group_id* with a unique value.

        The *dry* flag will simulate execution.  No output CSV will be
        created.

        Returns:
            number of messages read

        """
        log.debug('Topic "%s" dump set to read %s messages',
                  topic, max_read_count or 'all')
        log.debug('Topic dump timeout set to %d', self.timeout)

        with self.consumer(topic, group_id) as consumer:
            messages_read = 0
            for message in consumer:
                messages_read += 1
                sys.stdout.buffer.write(message.value)
                print()

                if (max_read_count is not None and
                        messages_read >= max_read_count):
                    log.info('Maximum read threshold %d breached - exiting',
                             max_read_count)
                    break

        return messages_read

    def reload_topic(self, target_dir, topic):
        """Source *target_dir* for files and reload file contents
        into a given Kafka *topic*.

        Returns:
            number of files read

        """
        files_processed = 0

        with self.producer() as producer:
            for target_file in domain_intel.utils.source_files(target_dir):
                with io.open(target_file, encoding='utf-8') as _fh:
                    contents = _fh.read()
                    producer.send(topic, contents.encode('utf-8'))
                    files_processed += 1

                    if not files_processed % 1000:
                        log.info('%d files reloaded', files_processed)

        return files_processed
