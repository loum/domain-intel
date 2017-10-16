"""Setup script for the Domain Intel Services project.
"""
import os
import setuptools


# Note: hardwiring requests to v2.11.1 to meet dependency of
# docker-compose v1.13.0.  This can be removed once docker-compose
# updates its deps list.
PROD_PACKAGES = [
    'backoff>=1.4.2',
    'future>=0.16.0',
    'gbq>=0.1.0',
    'kafka-python>=1.3.3',
    'logga>=1.0.2',
    'lxml>=3.7.3',
    'python-arango>=3.6.0',
    'python-dateutil>=2.6.1',
    'requests==2.11.1',
    'xlrd>=1.0.0',
    'xmljson>=0.1.7',
]

# Note: hardwiring docker-compose to v.1.11.0 due to python 3 bug
# as per https://github.com/docker/compose/issues/4729
DEV_PACKAGES = [
    'docker-compose==1.11.0',
    'mock>=2.0.0',
    'pylint',
    'pytest',
    'pytest-cov',
    'sphinx_rtd_theme',
    'twine',
    'Sphinx',
    'ipython==5.0.0',
]

PACKAGES = list(PROD_PACKAGES)
if (os.environ.get('APP_ENV') is not None and
        'dev' in os.environ.get('APP_ENV')):
    PACKAGES += DEV_PACKAGES

SETUP_KWARGS = {
    'name': 'domain-intel',
    'version': '0.6.1',
    'description': 'Domain Intel Services',
    'author': 'IP Echelon',
    'author_email': 'dev.sys@ip-echelon.com',
    'url': 'https://git1.ip-echelon.com/ipe/dis',
    'install_requires': PACKAGES,
    'packages': setuptools.find_packages(),
    'package_data': {
        'domain_intel': [
        ],
    },
    'scripts': [
        os.path.join('domain_intel', 'bin', 'ipe-dis')
    ],
    'license': 'MIT',
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
}

setuptools.setup(**SETUP_KWARGS)
