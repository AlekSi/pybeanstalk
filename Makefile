# Unit-testing, etc.

VIRTUALENV?=virtualenv

all: test_nose test_trial flakes pep8

env:
	rm -fr env
	mkdir -p .download_cache
	$(VIRTUALENV) --no-site-packages env
	env/bin/pip install --download-cache=.download_cache/ Twisted pyflakes pep8 nose pyyaml
	echo "\n\n>> Run 'source env/bin/activate'"

test_nose:
	env/bin/nosetests -v tests

test_trial:
	env/bin/trial tests/twisted_client/test_twisted_client.py

flakes:
	env/bin/pyflakes beanstalk

pep8:
	env/bin/pep8 -r beanstalk


.PHONY: env
