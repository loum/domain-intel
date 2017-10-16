"""Set project global scope.

"""
import os
import json
import io
import logging
from logga import log

# Quieten some of the logging from dependent modules.
logging.getLogger('kafka').setLevel(logging.ERROR)
logging.getLogger('arango').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('compose').setLevel(logging.ERROR)
logging.getLogger('docker').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.ERROR)

"""Configuration file for the logging module can be provided in the
following locations:

  * A place named by an environment variable `DIS_CONF`
  * System wide config directory - `/etc/domainintel/config.json`
  * Test config directory - `config/dev.json`

"""
CONFIG_LOCATIONS = [
    os.environ.get('DIS_CONF'),
    os.path.join(os.sep, 'etc', 'domainintel'),
    os.path.join(os.path.dirname(os.path.realpath(__file__)),
                 '..',
                 'config',
                 'dev.json'),
]

CONFIG = None
for location in CONFIG_LOCATIONS:
    if location is None:
        continue

    config_file = location
    if os.path.isdir(config_file):
        config_file = os.path.join(config_file, 'config.json')

    try:
        with io.open(config_file, encoding='utf-8') as _fh:
            log.info('Sourcing config from %s', config_file)
            CONFIG = json.loads(_fh.read())
            break
    except IOError as err:
        # Not a bad thing if the open failed.  Just means that the config
        # source does not exist.
        continue

if CONFIG is None:
    log.error('Domain Intel Services no config file found in locations: %s',
              ', '.join([x for x in CONFIG_LOCATIONS if x is not None]))

with io.open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          '..',
                          'config',
                          'country_codes.json'), encoding='utf-8') as _fh:
    COUNTRY_CODES = json.loads(_fh.read())

NS_20050711 = 'http://awis.amazonaws.com/doc/2005-07-11'
NS_20051005 = 'http://alexa.amazonaws.com/doc/2005-10-05/'
NS = {'a': NS_20051005, 'b': NS_20050711}
