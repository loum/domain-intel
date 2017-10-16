# Check if we have python3 available.
PY3_VERSION := $(shell python3 --version 2>/dev/null)
PY3_VERSION_FULL := $(wordlist 2, 4, $(subst ., , ${PY3_VERSION}))
PY3_VERSION_MAJOR := $(word 1, ${PY3_VERSION_FULL})
PY3_VERSION_MINOR := $(word 2, ${PY3_VERSION_FULL})
PY3_VERSION_PATCH := $(word 3, ${PY3_VERSION_FULL})

# python3.3 introduced the venv module which is the
# preferred method for creating python3 virtual envs.
# Otherwise, python3 defaults to pyvenv
USE_PYVENV := $(shell [ ${PY3_VERSION_MINOR} -ge 3 ] && echo 0 || echo 1)
ifneq ($(PY3_VERSION),)
  PY3 := $(shell which python3 2>/dev/null)
  ifeq ($(USE_PYVENV), 1)
    PY_VENV := pyvenv-${PY3_VERSION_MAJOR}.${PY3_VERSION_MINOR}
  else
    PY_VENV := ${PY3} -m venv
  endif
endif

# As long as pip has been installed system-wide, we can use virtualenv
# for python2.
PY2_VENV := $(shell which virtualenv 2>/dev/null)

# Determine virtual env tool to use.
ifeq ($(PYVERSION), 2)
  VENV_TOOL := ${PY2_VENV}
else
  VENV_TOOL := ${PY_VENV}
  PYVERSION := 3
endif

# OK, set some globals.
WHEEL=~/wheelhouse
PYTHONPATH=.
GIT=$(shell which git 2>/dev/null)

# Define the default test suit to run.
TESTS=domain_intel/test \
  domain_intel/awis/actions/test \
  domain_intel/awisapi/test \
  domain_intel/awisapi/actions/test \
  domain_intel/awisapi/parser/test \
  domain_intel/geodns/test \
  domain_intel/parser/test \
  domain_intel/analyst/test

test:
	PYTHONPATH=$(PYTHONPATH) DIS_CONF=config/dev.json \
  $(shell which py.test) -vv --exitfirst --cov-config .coveragerc \
  --pythonwarnings ignore --cov domain_intel \
  --junitxml junit.xml -sv $(TESTS)

docs:
	DIS_CONF=config/dev.json \
  $(shell which sphinx-build) -b html doc/source doc/out

clean:
	$(GIT) clean -xdf -e .vagrant -e *.swp -e 2env -e 3env

VENV_DIR_EXISTS := $(shell [ -e "${PYVERSION}env" ] && echo 1 || echo 0)
clear_env:
ifeq ($(VENV_DIR_EXISTS), 1)
	@echo \#\#\# Deleting existing environment ${PYVERSION}env ...
	$(shell which rm) -fr ${PYVERSION}env
	@echo \#\#\# ${PYVERSION}env delete done.
endif

init_env:
	@echo \#\#\# Creating virtual environment ${PYVERSION}env ...
	@echo \#\#\# Using wheel house $(WHEEL) ...
ifneq ($(VENV_TOOL),)
	$(VENV_TOOL) ${PYVERSION}env
	@echo \#\#\# ${PYVERSION}env build done.

	@echo \#\#\# Preparing wheel environment and directory ...
	$(shell which mkdir) -pv $(WHEEL) 2>/dev/null
	$(PYVERSION)env/bin/pip install --upgrade pip
	$(PYVERSION)env/bin/pip install --upgrade setuptools
	$(PYVERSION)env/bin/pip install wheel
	@echo \#\#\# wheel env done.

	@echo \#\#\# Installing package dependencies ...
	$(PYVERSION)env/bin/pip wheel --wheel-dir $(WHEEL) --find-links=$(WHEEL) .
	$(PYVERSION)env/bin/pip install --use-wheel --find-links=$(WHEEL) -e .
	@echo \#\#\# Package install done.
else
	@echo \#\#\# Hmmm, cannot find virtual env tool.
	@echo \#\#\# Virtual environment not created.
endif

init: clear_env init_env

init_build: init_env build

build:
	@echo \#\#\# Building package ...
	$(PYVERSION)env/bin/python setup.py bdist_wheel -d $(WHEEL)
	@echo \#\#\# Build done.

py_versions:
	@echo python3 version: ${PY3_VERSION}
	@echo python3 minor: ${PY3_VERSION_MINOR}
	@echo path to python3 executable: ${PY3}
	@echo python3 virtual env command: ${PY_VENV}
	@echo python2 virtual env command: ${PY2_VENV}
	@echo virtual env tooling: ${VENV_TOOL}

print-%:
	@echo '$*=$($*)'

.PHONY: test docs py_versions init build upload
