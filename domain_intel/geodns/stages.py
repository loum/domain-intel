""":class:`GeoDNSStage`
"""
from collections import Counter
import inspect
import time
import signal
import os
import errno
import multiprocessing
import unicodedata
from logga import log

import domain_intel.utils
import domain_intel.parser
import domain_intel.common
from domain_intel.geodns import GeoDNS, GeoDNSError, CheckHostNetError, CompassServerError


DUMP_PUBLISH="publish"
DUMP_CONSUME="consume"

class WorkerTimedOut(Exception):
    pass


class GeoDNSStage(object):
    """joins a single stage of GeoDNS processing and the kafka queues necessary to complete it"""

    # TODO: important doco here
    def __init__(
            self,
            worker=None,
            worker_timeout_seconds=0,
            kafka_consumer_topics=None,
            kafka_producer_topics=None,
            kafka_consumer_group_id=None,
            kafka_consumer=None,
            kafka_consumer_factory=None,
            kafka_producer=None,
            kafka_producer_factory=None,
            max_read_count=None,
            dry=False,
            dump=None,
            retryable_exceptions=None,
            retryable_exceptions_count=10
    ):
        self.metrics = Counter()
        self.worker = worker
        self.worker_timeout_seconds = worker_timeout_seconds
        self.max_read_count = max_read_count
        self.kafka_consumer_topics = kafka_consumer_topics
        self.kafka_consumer_group_id = kafka_consumer_group_id
        self.kafka_producer_topics = kafka_producer_topics
        self.dry = dry
        self.dump = dump

        if self.dump:
            for dir in ( dump, os.path.join(dump, DUMP_PUBLISH), os.path.join(dump, DUMP_CONSUME) ):
                try:
                    os.makedirs(dir)
                except OSError as exc:
                    if exc.errno == errno.EEXIST and os.path.isdir(dir):
                        pass
                    else:
                        raise

        self.retryable_exceptions_count = retryable_exceptions_count
        if retryable_exceptions is not None:
            self.retryable_exceptions = tuple(retryable_exceptions)
        else:
            self.retryable_exceptions = tuple()

        # override group_id if in dry run mode
        if self.dry:
            self.kafka_consumer_group_id = domain_intel.utils.id_generator()

        # these may be none, we'll initialise when we try to use them
        self.kafka_consumer = kafka_consumer
        self.kafka_consumer_factory = kafka_consumer_factory
        self.kafka_producer = kafka_producer
        self.kafka_producer_factory = kafka_producer_factory

        self.bootstrap_servers = domain_intel.common.CONFIG.get('kafka')["bootstrap_servers"]

    @property
    def is_consumer(self):
        return self.kafka_consumer_topics is not None and len(self.kafka_consumer_topics) > 0

    @property
    def is_producer(self):
        return self.kafka_producer_topics is not None and len(self.kafka_producer_topics) > 0

    def _init_kafka(self):
        """initialise kafka consumer/producer where appropriate"""
        # TODO: handle reconnect here? or let it fail

        if self.kafka_consumer is None and self.is_consumer:
            if self.kafka_consumer_factory is not None:
                self.kafka_consumer = self.kafka_consumer_factory()
            else:
                # must exceed our # of retries * timeout with a little to spare
                if self.worker_timeout_seconds > 0:
                    k_timeout = self.worker_timeout_seconds * (self.retryable_exceptions_count + 1) * 1000
                else:
                    k_timeout = (5 * 60 * 1000)

                # side step context manager, must close explicitly now
                self.kafka_consumer = domain_intel.utils._safe_consumer(
                    # topic can take a list of topics or scalar
                    topic=self.kafka_consumer_topics,
                    group_id=self.kafka_consumer_group_id,
                    enable_auto_commit=False,
                    bootstrap_servers=self.bootstrap_servers,
                    session_timeout_ms=k_timeout,
                    request_timeout_ms=k_timeout*2,
                )

        if self.kafka_producer is None and self.is_producer:
            if self.kafka_producer_factory is not None:
                self.kafka_producer = self.kafka_producer_factory()
            else:
                # side step context manager, must close explicitly now
                self.kafka_producer = domain_intel.utils._safe_producer(
                    bootstrap_servers=self.bootstrap_servers,
                )

    def _do_dump(self, payload, offset, subdir):
        log.debug("DUMPING TO %s/%s/%s with value: %s", self.dump, subdir, offset, payload)
        with open("%s/%s/%s" % (self.dump, subdir, offset), "wb") as _fh:
            _fh.write(payload)

    def publish(self, payloads):
        """publish arbitrary data into producer. use case would be if
        this is the first stage in a pipeline and doesnt read from anywhere"""

        if not self.is_producer:
            raise GeoDNSError("cannot publish without > 0 topics")

        self._init_kafka()
        metrics = self.metrics

        for i, payload in enumerate(payloads):
            log.debug("publishing %s", payload)
            for dest_topic in self.kafka_producer_topics:
                if not self.dry:
                    self.kafka_producer.send(dest_topic, value=payload)
                else:
                    log.debug("%s: %s", dest_topic, payload)
                    if self.dump:
                        self._do_dump(payload, str(i), DUMP_PUBLISH)

                metrics[dest_topic] += 1
        self.kafka_producer.flush()

        return metrics

    @classmethod
    def _timeout_handler(*args, **kwargs):
        raise WorkerTimedOut("hit timeout")

    def run(self):
        self._init_kafka()

        # preflight checks, since run presumes and input and output side
        # we must validate that we have what we need.
        # this is not done in the constructor to support special case stages
        # i.e. root and final leaf node
        if self.kafka_consumer_group_id is None:
            raise GeoDNSError("will not accept null kafka_consumer_group_id. set one if you are consuming")

        if self.worker is None:
            raise GeoDNSError("need a worker!")

        if not self.is_producer and self.is_consumer:
            raise GeoDNSError("cannot call run() without input and output topics")

        self.kafka_consumer.subscribe(self.kafka_consumer_topics)

        metrics = self.metrics
        for msg in self.kafka_consumer:
            metrics["messages_received"] += 1

            if self.dump:
                self._do_dump(msg.value, str(metrics["messages_received"]), DUMP_CONSUME)

            last_exc = None
            for retry in range(0, self.retryable_exceptions_count):
                try:

                    # enforce process level timeout with signals
                    old_alarm_handler = signal.signal(signal.SIGALRM, GeoDNSStage._timeout_handler)
                    signal.alarm(self.worker_timeout_seconds)

                    res = self.worker(msg.value)

                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_alarm_handler)

                    last_exc = None
                    break
                except self.retryable_exceptions + (WorkerTimedOut,) as exc:
                    log.error("caught retryable exceptions: %s", str(exc))
                    metrics["retryable_exceptions"] += 1
                    last_exc = exc
                    time.sleep(retry)

            if last_exc is not None:
                log.error("exceeded retryable exception count of %d", self.retryable_exceptions_count)
                raise last_exc

            # try marshalling response
            metrics["messages_processed"] += 1
            if hasattr(res, "marshal"):
                res = res.marshal()
                metrics["responses_marshalled"] += 1

            for dest_topic in self.kafka_producer_topics:
                metrics["messages_sent"] += 1

                if not self.dry:
                    self.kafka_producer.send(dest_topic, value=res)
                else:
                    log.debug("%s: %s", dest_topic, res)
                    if self.dump:
                        self._do_dump(res, "%d.%d" % (metrics["messages_received"], metrics["messages_sent"]), DUMP_PUBLISH)

            self.kafka_producer.flush()
            self.kafka_consumer.commit()

            log.debug(metrics)

            if self.max_read_count is not None and metrics["messages_received"] >= self.max_read_count:
                break

        return metrics

    def persist(self):
        """Persist flattened (processed) GeoDNS data to ArangoDB.

        :attr:`max_read_count` can limit the number of records read from
        *topic*.  The default action is to read all available messages.

        The default consumer :attr:`topics` is `dns-geodns-parsed`.

        The :attr:`dry` flag will simulate execution.  No records will be
        published.

        Returns:
            total count of records written to the DB across all workers

        """
        count_q = multiprocessing.Queue()

        target = self.persist_worker
        args = (
            count_q,
            self.max_read_count,
            self.kafka_consumer_topics[0],
            self.kafka_consumer_group_id
        )
        kwargs = {'dry': self.dry}
        threads = domain_intel.common.CONFIG.get('threads', 1)
        domain_intel.utils.threader(threads, target, *args, **kwargs)

        total_count = 0
        while not count_q.empty():
            total_count += count_q.get()

        log.debug('Persisted GeoDNS total count %d', total_count)

        return total_count

    def persist_worker(self,
                       queue,
                       max_read_count,
                       topic,
                       group_id,
                       dry=False):
        """Persist flattened (processed) GeoDNS domain data to ArangoDB
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
        timeout = domain_intel.common.CONFIG.get('timeout', 10000)
        log.debug('Persist worker timeout set to %d', timeout)

        store = domain_intel.Store()

        kafka_config = domain_intel.common.CONFIG.get('kafka', {})
        kwargs = {
            'bootstrap_servers': kafka_config.get('bootstrap_servers'),
            'group_id': group_id,
            'consumer_timeout_ms': timeout,
        }
        with domain_intel.utils.safe_consumer(topic, **kwargs) as consumer:
            messages_read = 0
            for message in consumer:
                messages_read += 1

                dns_data = message.value.decode('utf-8')
                parser = domain_intel.parser.GeoDNS(dns_data)
                store.collection_insert('geodns',
                                        parser.db_geodns_raw(),
                                        dry)

                for ipv4 in parser.db_ipv4_vertex:
                    store.collection_insert('ipv4', ipv4, dry)

                for ipv6 in parser.db_ipv6_vertex:
                    store.collection_insert('ipv6', ipv6, dry)

                for ipv4_edge in parser.db_ipv4_edge:
                    store.edge_insert('ipv4_resolves', ipv4_edge, dry)

                for ipv6_edge in parser.db_ipv6_edge:
                    store.edge_insert('ipv6_resolves', ipv6_edge, dry)

                if (max_read_count is not None and
                        messages_read >= max_read_count):
                    log.info('Maximum read threshold %d breached - exiting',
                             max_read_count)
                    break

        log.debug('Data persist worker domains read %d', messages_read)

        queue.put(messages_read)


def load_domains_from_file(filename, **kwargs):
    """Load GTR domains into 'dns-domains' Kafka topic"""

    stage = GeoDNSStage(
        kafka_producer_topics=["dns-domains"],
        **kwargs
    )

    log.info("loading domains to %s from file %s", stage.kafka_producer_topics, filename)

    def _chomped_lines(_filename):
        with open(_filename, "rb") as _fh:
            for line in _fh:
                yield line.rstrip()

    metrics = stage.publish(_chomped_lines(filename))
    log.info("finished loading domains from %s to %s with %s", stage.kafka_producer_topics, filename, metrics)

    return metrics


def slurp_domains_dns(dry=False, **kwargs):
    group_id = inspect.stack()[0][3]
    if kwargs["dump"] is not None and kwargs["dump"] != "":
        group_id = domain_intel.utils.id_generator()

    # first stage of pipeline (dns-domain to dns-raw) should run
    stage = GeoDNSStage(
        kafka_consumer_topics=["dns-domains"],
        kafka_producer_topics=["dns-raw"],
        kafka_consumer_group_id=group_id,
        worker=GeoDNS(
            compass_username=domain_intel.common.CONFIG["geodns"]["compass"]["username"],
            compass_password=domain_intel.common.CONFIG["geodns"]["compass"]["password"],
        ).resolve_dns,
        worker_timeout_seconds=15,
        retryable_exceptions=(CheckHostNetError,),
        **kwargs
    )
    log.info("slurping dns results from %s to %s", stage.kafka_producer_topics, stage.kafka_consumer_topics)

    return stage.run()


def flatten_dns_raw(**kwargs):
    group_id = inspect.stack()[0][3]
    if kwargs["dump"] is not None and kwargs["dump"] != "":
        group_id = domain_intel.utils.id_generator()

    stage = GeoDNSStage(
        kafka_consumer_topics=["dns-raw"],
        kafka_producer_topics=["dns-parsed"],
        kafka_consumer_group_id=group_id,
        worker=GeoDNS(
            compass_username=domain_intel.common.CONFIG["geodns"]["compass"]["username"],
            compass_password=domain_intel.common.CONFIG["geodns"]["compass"]["password"],
        ).parse_checkhostnetresult,
        **kwargs
    )
    log.info("flattening dns results from %s to %s", stage.kafka_producer_topics, stage.kafka_consumer_topics)

    return stage.run()


def slurp_and_flatten_geodns(**kwargs):
    group_id = inspect.stack()[0][3]
    if kwargs["dump"] is not None and kwargs["dump"] != "":
        group_id = domain_intel.utils.id_generator()

    stage = GeoDNSStage(
        kafka_consumer_topics=["dns-parsed"],
        kafka_producer_topics=["dns-geodns-parsed"],
        kafka_consumer_group_id=group_id,
        worker=GeoDNS(
            compass_username=domain_intel.common.CONFIG["geodns"]["compass"]["username"],
            compass_password=domain_intel.common.CONFIG["geodns"]["compass"]["password"],
        ).resolve_geog_from_dns,
        #worker_timeout_seconds=180,
        retryable_exceptions=(CompassServerError,),
        **kwargs
    )
    log.info("slurping geog results from %s to %s", stage.kafka_producer_topics, stage.kafka_consumer_topics)

    return stage.run()
