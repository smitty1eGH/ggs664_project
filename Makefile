.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

help:
	@echo ""
	@echo "Handy project commands:"
	@echo ""
	@echo "target   | description"
	@echo "---------+------------"
	@echo "eserv      Run emacs daemon in venv"
	@echo "ecli       Run emacsclient after eserv is running"
	@echo "clean      remove all build, test, coverage and Python artifacts"
	@echo "lint       check style with flake8"
	@echo "test       run tests quickly with the default Python"
	@echo "test-all   run tests on every Python version with tox"
	@echo "coverage   check code coverage quickly with the default Python"
	@echo "docs       generate Sphinx HTML documentation, including API docs"
	@echo "servedocs  compile the docs watching for changes"
	@echo "release    package and upload a release"
	@echo "dist       builds source and wheel package"
	@echo "install    install the package to the active Python's site-packages"
	@echo ""
eserv:
	@find ~/.emacs.d -type f -name ".elc" -delete && emacs --daemon && echo "\n" && emacsclient -t --eval '(dired-jump)'
ecli:
	@emacsclient -t --eval '(dired-jump)'

clean: clean-build clean-pyc clean-test

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg'      -exec rm -fr {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc'       -exec rm  -f {} +
	find . -name '*.pyo'       -exec rm  -f {} +
	find . -name '*~'          -exec rm  -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm  -f .coverage
	rm -fr  htmlcov/

lint: ## check style with flake8
	flake8 cedar2666 tests

test: ## run tests quickly with the default Python
	python setup.py test

test-all: ## run tests on every Python version with tox
	tox

coverage: ## check code coverage quickly with the default Python
	coverage run --source cedar2666 setup.py test
	coverage report -m
	coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/cedar2666.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ cedar2666
	$(MAKE)       -C docs  clean
	$(MAKE)       -C docs  html
	$(BROWSER)       docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: clean ## package and upload a release
	python setup.py sdist upload
	python setup.py bdist_wheel upload

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

install: clean ## install the package to the active Python's site-packages
	python setup.py install
	@rm -vf __pycache__/*
