#!/bin/bash
set -eux

createuser -h $PGHOST -U postgres -dl $PGUSER || echo "User $PGUSER already exists, skipping"
dropdb -h $PGHOST -U postgres $PGDBNAME || echo "$PGDBNAME hasn't created before"
createdb -h $PGHOST -U postgres -O $PGUSER $PGDBNAME

psql -d $PGDBNAME -f ./Schema/001-initial.blue.sql

PYTHONPATH=./Python pyre --source-directory ./Python --search-path "$(pipenv --venv)/lib/python3.6/site-packages/" check
PYTHONPATH=./Python pytest -v --cov-report term-missing --cov=. --cov-config .coveragerc $*
