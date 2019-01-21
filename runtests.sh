#!/bin/bash
set -eux

# createuser -h $PGHOST -U postgres -dl $PGUSER || echo "User $PGUSER already exists, skipping"
# dropdb -h $PGHOST -U postgres $PGDBNAME || echo "$PGDBNAME hasn't created before"
# createdb -h $PGHOST -U postgres -O $PGUSER $PGDBNAME
PGDBNAME="test_mod_ngarn"
dropdb -U postgres $PGDBNAME || echo "$PGDBNAME hasn't created before"
createdb -U postgres $PGDBNAME

# pyre --source-directory ./mod_ngarn --search-path "$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")" check
PGDBNAME=$PGDBNAME pytest -v --cov-report term-missing --cov=. --cov-config .coveragerc $*
