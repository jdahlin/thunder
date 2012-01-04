all: check

test: clean_coverage
	@nosetests --all-modules --with-coverage --cover-package=thunder thunder

clean_coverage:
	@rm -f .coverage

coverage:
	@echo 'Generating coverage statistics in coverage_html/index.html ...'
	@coverage html -d coverage_html \
	    --include=thunder/* \
	    --omit=thunder/tests/*

clean: clean_coverage
	@echo 'Cleaning...'
	@find . -name "*.pyc" -exec rm -f {} \;
	@echo 'Done.'

pep8:
	@echo 'Checking pep8 compliance...'
	@pep8 -r thunder

pyflakes:
	@echo 'Running pyflakes...'
	@pyflakes thunder

check: pep8 pyflakes test
	@grep ^TOTAL tests_output/test.log | grep 100% >/dev/null || \
	{ echo 'Unit tests coverage is incomplete.'; exit 1; }

sphinx:
	@sphinx-apidoc -o apidocs -F -H thunder -A thunder -V 1.0 -R 1.0 thunder
	@cd apidocs && make clean && PYTHONPATH=.. make html && cd ..
