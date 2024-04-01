#!/usr/bin/env bash

set -e

echo "Run test with coverage"
coverage run -m pytest -v tests

echo "Generate coverage report[XML]"
coverage xml