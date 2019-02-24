# mod-ngarn
ModNgarn – Simple job workers

## Usage
```
Usage: mod-ngarn [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create-table
  run
```

## Installation
```
pip install mod-ngarn
```

## Run modngarn
```
Usage: mod-ngarn run [OPTIONS]

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

## Dev
### Required
- pipenv (https://github.com/pypa/pipenv)
- running PostgreSQL (`psql` should work)
- python 3.7

#### Setup
```
pipenv install --python 3.7
pipenv shell
```

#### Runtests
```
./runtests.sh
```

#### Publish to PyPi
```
flit publish
```
