code-quality:
	flake8 .
	isort .
	black .

merge-validate:
	pytest
	python -m build && twine check dist/*

full-check: code-quality merge-validate