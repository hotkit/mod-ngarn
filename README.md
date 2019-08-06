# mod-ngarn

[![CircleCI](https://circleci.com/gh/Proteus-tech/mod-ngarn.svg?style=svg)](https://circleci.com/gh/Proteus-tech/mod-ngarn) [![PyPI version](https://badge.fury.io/py/mod_ngarn.svg)](https://badge.fury.io/py/mod_ngarn)

## Usage
```
Usage: mod-ngarn [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create-table     Create mod-ngarn queue table
  delete-job       Delete executed task
  run              Run mod-ngarn job
  wait-for-notify  Wait and listening for NOTIFY
```

## Installation
```
pip install mod-ngarn
```

## Run modngarn
```
Usage: mod-ngarn run [OPTIONS]

  Run mod-ngarn job

Options:
  --queue-table TEXT  Queue table name (Default: os.getenv("DBTABLE",
                      "public.modngarn_job"))
  --limit INTEGER     Limit jobs (Default: 300)
  --max-delay FLOAT   Max delay for failed jobs (seconds) (Default: None)
  --help              Show this message and exit.
```

## Create modngarn job queue table
```
Usage: mod-ngarn create-table [OPTIONS]

Options:
  --queue-table TEXT  Queue table name (Default: os.getenv("DBTABLE",
                      "public.modngarn_job"))
  --help              Show this message and exit.
```

## Wait for notify
```
Usage: mod-ngarn wait-for-notify [OPTIONS]

  Wait and listening for NOTIFY

Options:
  --queue-table TEXT  Queue table name (Default: os.getenv("DBTABLE",
                      "public.modngarn_job"))
  --help              Show this message and exit.
```

## Delete executed job
```
Usage: mod-ngarn delete-job [OPTIONS]

  Delete executed task

Options:
  --queue-table TEXT  Queue table name (Default: os.getenv("DBTABLE",
                      "public.modngarn_job"))
  --help              Show this message and exit.
```

## Example run script
```
#!/bin/bash
set -u

while true
do
    mod-ngarn run $*
    timeout 5 mod-ngarn wait-for-notify
done
```

## Dev
### Required
- flit (https://pypi.org/project/flit/)
- running PostgreSQL (`psql` should work)
- python 3.7

#### Setup
```
flit install
```

#### Runtests
```
./runtests.sh
```

#### Publish to PyPi
```
flit publish
```
