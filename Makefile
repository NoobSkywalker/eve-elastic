SHELL=/bin/bash

# fixed missing tmpdir var for linux
TMPDIR ?= /tmp

# workspace used for test/coverage result output
WORKSPACE ?= ${TMPDIR}

PKG_NAME = eve_elastic

# use workon home if found in environment variables or default to local env
WORKON_HOME ?= ${WORKSPACE}/.venv/
VIRTUAL_ENV ?= $(WORKON_HOME)/$(PKG_NAME)

pyenv = $(VIRTUAL_ENV)
pylama = $(pyenv)/bin/pylama
nose = $(pyenv)/bin/nosetests
py_namespace = eve_elastic

$(info using python environment: $(pyenv))

# location for spamclass pip repository and additional python packages
PY_VERSION = python3.5
SITE_PACKAGES = $(pyenv)/lib/$(PY_VERSION)/site-packages)

# python binaries which are located in the package virtualenv
# python command to use
python = $(pyenv)/bin/python3

python_build = $(python_build_environment) $(python)
pip = $(python_build_environment) $(pyenv)/bin/pip

# other binaries
virtualenv = virtualenv -p `which $(PY_VERSION)` -q --clear

python_files = $(shell find eve_elastic -name \*.py)


install_dev:
	# install dependencies if you do not have them (python3.5)
	sudo apt-get -y install python3.5 python-dev
	sudo pip3 install virtualenv


install: $(python_files) setup.py | $(pyenv)

# Testing
pytest: install cleantestpycache
	# run project unit tests
	$(python) $(nose) -x --verbosity=2 test/test_elastic.py

$(pyenv): requirements.txt
	@rm -rf $(pyenv)/.*.done
	$(virtualenv) $(pyenv)
	$(pip) install -q --upgrade pip
	$(pip) install --exists-action=w -qr requirements.txt
	@touch $@

# cleanup pycache files in tests directory to make
# working with local-dev/vagrant at the same time easier
cleantestpycache:
	find test -name __pycache__ | xargs rm -rf


## private targets
pycheck: | $(pytest) $(pyenv)
	$(pylama) $(py_namespace) tests setup.py $(TEST_ARGS)


## public targets

# linting
check: cleantestpycache pycheck

# run nosetests
test: cleantestpycache pytest


# Housekeeping

clean:
	rm -rf *.xml .cache tmp htmlcov

mrproper: clean
	# python environment and state files
	rm -rf $(pyenv) .*.done ${TMPDIR}/deb_dist deps
