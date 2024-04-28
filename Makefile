run-tests:
	coverage run -m pytest -v tests
	coverage report

check-quality:
	flake8 .
	isort .
	black .

test-build:
	python -m build && twine check dist/*

validate-merge: run-tests test-build
	python -m build && twine check dist/*

# TODO: introduce --project-dir param for the following to work
# regression-test:
# 	tulona run

# full-check: code-quality merge-validate regression-test
full-check: check-quality validate-merge
