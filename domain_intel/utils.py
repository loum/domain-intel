"""General utils.

"""
import os
import io
import json
import collections
import fnmatch
import random
import string
import inspect
import multiprocessing
import contextlib
import time
import datetime
import calendar
from dateutil.relativedelta import relativedelta
import pytz
import kafka
import backoff
import xlrd
from logga import log

import domain_intel
import domain_intel.common

CONFIG = domain_intel.common.CONFIG
TOPICS = CONFIG.get('kafka')['topics'].split(',')


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """Random string generator.

    *size* sets the length of the random string (default 6).  *chars*
    sets the character set to use in the generation of the string (default
    all uppercase and digits).

    Typical usage::

        >>> import domain_intel
        >>> from domain_intel.utils import id_generator
        >>> id_generator()
        'V4WA7H'

    **Returns:**
        random string of length *size*

    """
    return ''.join(random.choice(chars) for _ in range(size))


def get_file_line_count(file_h):
    """Count number of lines in a file.

    Useful for logging state when processing large files.

    Will reset the seek to the start of the file once complete.

    **Args:**
        *file_h*: file object to the source file

    **Returns:**
        number of lines in the file

    """
    lines_in_file = 0
    try:
        lines_in_file = list(enumerate(file_h, 1))[-1][0]
    except IndexError:
        pass
    log.info('Total lines in file: %d', lines_in_file)

    file_h.seek(0)

    return lines_in_file


def threader(thread_count, target, *args, **kwargs):
    """Common process thread creator.

    *thread_count* controls the number of processes to launch.  *target*
    is a reference to the executable component to thread.  *args* and
    *kwargs* feed into the *target*'s parameter signature.

    All threads will be joined on startup so the completion of this
    function indicates a exit event from all threads.

    Typical example::

        >>> from domain_intel.utils import threader
        >>> target = print
        >>> args = ('Hello', 'there')
        >>> threader(2, target, *args)
        Hello there
        Hello there

    """
    threads = []
    for _ in range(thread_count):
        worker = multiprocessing.Process(target=target,
                                         args=list(args),
                                         kwargs=dict(kwargs))
        worker.start()
        threads.append(worker)

    for _thread in threads:
        _thread.join()


@backoff.on_exception(backoff.expo,
                      (kafka.errors.KafkaError, OSError),
                      max_tries=20)
def _safe_consumer(topic, **kwargs):
    """See :func:`safe_consumer`.  This returns a direct consumer
    without context manager.

    """
    caller = inspect.stack()[2][3]
    log.info('Starting consumer for %s', caller)

    default_kwargs = {
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'consumer_timeout_ms': 10000,
    }
    default_kwargs.update(dict(kwargs))
    consumer = kafka.consumer.KafkaConsumer(**default_kwargs)
    if topic is not None:
        consumer.subscribe(topic)
    return consumer


@contextlib.contextmanager
def safe_consumer(topic, **kwargs):
    """Obtain a Kafka producer safely.  Waits until all brokers
    and topics are online.

    """
    caller = inspect.stack()[2][3]
    consumer = _safe_consumer(topic, **kwargs)
    yield consumer

    log.info('Closing consumer for %s ...', caller)
    consumer.close(10)
    log.info('Consumer closed for caller %s', caller)


@backoff.on_exception(backoff.expo,
                      (kafka.errors.KafkaError, OSError),
                      max_tries=20)
def _safe_producer(**kwargs):
    """See :func:`safe_producer`.  This returns a direct producer
    without context manager.

    """
    caller = inspect.stack()[2][3]
    log.info('Starting producer for %s', caller)
    return kafka.producer.KafkaProducer(**kwargs)


@contextlib.contextmanager
def safe_producer(**kwargs):
    """Obtain a Kafka producer safely.  Waits until all brokers
    and topics are online.

    """
    caller = inspect.stack()[2][3]
    producer = _safe_producer(**kwargs)
    yield producer

    log.info('Closing producer for %s ...', caller)
    producer.flush()
    producer.close(10)
    log.info('Producer closed for caller %s', caller)


@backoff.on_exception(backoff.expo,
                      kafka.errors.KafkaError,
                      max_tries=20)
@backoff.on_predicate(backoff.fibo,
                      lambda x: len(x) < len(TOPICS),
                      max_value=13)
def info(**kwargs):
    """Simple dump to logs/stout of information related to the topics
    we are currently authorised to access.

    """
    log.info('Attempting get of Kafka topic detail information ...')
    with safe_consumer(None, **kwargs) as consumer:
        topics = consumer.topics()
        topic_count = len(topics)
        for index, topic in enumerate(topics, 1):
            log.debug('Authorised topic %d of %d: %s',
                      index, topic_count, topic)
            partitions = [str(x) for x in consumer.partitions_for_topic(topic)]
            log.info('- Partitions: %s', ', '.join(partitions))

    return topics


def stabilise_partitions(topics, **kwargs):
    """In addition to topic creation, we need to pause and wait for
    the *topics* partitions to be created.

    """
    log.info('Stabilising Kafka topic partitions ...')
    with safe_producer(**kwargs) as producer:
        for topic in topics:
            log.info('Waiting for topic "%s" partitions ...', topic)
            producer.partitions_for(topic)


def source_files(source_dir=os.curdir, file_filter='*'):
    """Returns files in the directory given by :attr:`source_dir`.

    Does not include the special entries '.' and '..'.

    If :attr:`file_filter` is provided, will perform a regular
    expression match against the files within :attr:`source_dir`.

    Returns:
        each file in the directory as a generator

    """
    log.info('Searching path "%s" for files matching filter "%s"',
             source_dir, file_filter)

    for dir_info in os.walk(os.path.abspath(source_dir)):
        path = dir_info[0]
        files = dir_info[2]
        for filename in fnmatch.filter(files, file_filter):
            log.info('Matched file "%s"', os.path.join(path, filename))
            yield os.path.join(path, filename)


def standardise_file_handle(file_h):
    """Standardise file object reference *file_h* across Python 2 and 3.

    Returns:
        :class:`_io.TextIOWrapper` file object

    """
    filename = file_h.name
    file_h.close()
    new_file_h = io.open(filename, encoding='utf-8')

    return new_file_h


def epoch_from_str(date, date_format='%Y-%m-%d'):
    """Given a string representation of *date*, returns the time since
    epoch equivalent as interpreted by *format*.

    Current implementation is Python 2 and 3 compatible.  However, same
    could be achieved with following entirely in Python 3::

        _dt.replace(tzinfo=.timezone.utc).timestamp()

    *date* is interpreted as timezone agnostic.

    Returns:
        seconds since epoch

    """
    timestamp = None

    try:
        _dt = datetime.datetime.strptime(date, date_format)
        timestamp = _dt.replace(tzinfo=pytz.UTC)
        timestamp = (_dt - datetime.datetime(1970, 1, 1)).total_seconds()
    except ValueError as err:
        log.error('Epoch conversion of "%s" with format "%s" error: %s',
                  date, date_format, err)

    return timestamp


def analyst_xls_to_json(xls_file, dry=False):
    """Convert *xls_file* into a JSON file.
    Conversion will attempt to create the JSON file variant in the same
    directory as *xls_file*.

    If *dry* is ``True``, will attempt to write out the converted JSON to
    a filename based on *xls_file* with the ``xls`` extension replaced
    by ``json``.

    Returns:
        converted XLS content as JSON

    """
    log.info('Attempting to convert xls file: "%s" to JSON', xls_file)

    filename = os.path.splitext(xls_file)[0]

    workbook = xlrd.open_workbook(xls_file)
    sheet = workbook.sheet_by_index(1)

    data = collections.OrderedDict()
    for rownum in range(1, sheet.nrows):
        row_values = sheet.row_values(rownum)
        if row_values:
            domain = row_values[0]
            data.setdefault(domain, {})
            data[domain]['p2p_magnet_links'] = row_values[1]
            data[domain]['links_to_torrents'] = row_values[2]
            data[domain]['links_to_osp'] = row_values[3]
            data[domain]['search_feature'] = row_values[4]
            data[domain]['domain_down_or_parked'] = row_values[5]
            data[domain]['has_rss_feed'] = row_values[6]
            data[domain]['requires_login'] = row_values[7]
            data[domain]['has_forum_or_comments'] = row_values[8]

    if not dry:
        target_file = '{}.json'.format(filename)
        file_h = io.open(target_file, 'w', encoding='utf-8')
        file_h.write(json.dumps(data, indent=2))
        file_h.close()

    for key, value in data.items():
        yield json.dumps({key: value})


def get_epoch_ranges(months=0):
    """Define epoch time ranges ending last day last month and starting
    first day less *months* months before.

    Epcoh calculations with always assume month preceding current month.

    Example::

        >>> import domain_intel.utils
        >>> domain_intel.utils.get_epoch_ranges()
        (1498867200.0, 1501459200.0)
        >>>

    Returns:
        tuple structure representing start and end epoch times

    """
    current_epoch = calendar.timegm(time.gmtime())
    _dt = datetime.datetime.utcfromtimestamp(current_epoch)

    first = _dt.replace(day=1)
    end_month = first - datetime.timedelta(days=1)

    start_month = end_month - relativedelta(months=months)
    first_day_start_month = start_month.replace(day=1)

    start_fmt = first_day_start_month.strftime('%Y-%m-%d')
    start_epoch = domain_intel.utils.epoch_from_str(start_fmt)
    end_fmt = end_month.strftime('%Y-%m-%d')
    end_epoch = domain_intel.utils.epoch_from_str(end_fmt)

    return (start_epoch, end_epoch)
