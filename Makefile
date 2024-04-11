run-tests:
	coverage run -m pytest -v tests
	coverage report

code-quality:
	flake8 .
	isort .
	black .

merge-validate: run-tests
	python -m build && twine check dist/*

# TODO: introduce --project-dir param for the following to work
# regression-test:
# 	tulona ping --datasources employee_postgres
# 	tulona ping -v --datasources employee_postgres
# 	tulona ping --datasources employee_postgres,employee_mysql
# 	tulona profile --datasources employee_postgres,employee_mysql
# 	tulona profile --compare --datasources employee_postgres,employee_mysql
# 	tulona compare-data --datasources employee_postgres,employee_mysql
# 	tulona compare-data --sample-count 50 --datasources employee_postgres,employee_mysql
# 	tulona compare-column --datasources employee_postgres,employee_mysql
# 	tulona compare --datasources employee_postgres,employee_mysql
# 	tulona compare --sample-count 50 --datasources employee_postgres,employee_mysql

# full-check: code-quality merge-validate regression-test
full-check: code-quality merge-validate
