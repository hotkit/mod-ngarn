#!/bin/bash
set -eux

dropdb mod-ngarn-test || echo "OK, mod-ngarn-test database hasn't been created before"
createdb mod-ngarn-test

psql -d mod-ngarn-test -f ./Schema/001-initial.blue.sql


PYTHONPATH=./Python pytest -v --cov-report term-missing --cov=. --cov-config .coveragerc $*
