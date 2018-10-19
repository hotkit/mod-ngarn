#!/bin/bash
set -eux

# createuser -h $PGHOST -U postgres -dl $PGUSER || echo "User $PGUSER already exists, skipping"
# dropdb -h $PGHOST -U postgres $PGDBNAME || echo "$PGDBNAME hasn't created before"
# createdb -h $PGHOST -U postgres -O $PGUSER $PGDBNAME
PGDBNAME="test_mod_ngarn"
dropdb -U postgres $PGDBNAME || echo "$PGDBNAME hasn't created before"
createdb -U postgres $PGDBNAME

psql -d $PGDBNAME -f ./Schema/001-initial.blue.sql

pyre --source-directory . --search-path "$(pipenv --venv)/lib/python3.7/site-packages/" check
PGDBNAME=$PGDBNAME pytest -v --cov-report term-missing --cov=. --cov-config .coveragerc $*
