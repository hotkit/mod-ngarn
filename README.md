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

## Create modngarn job table
```
Usage: mod-ngarn create-table [OPTIONS]

Options:
  --name TEXT  mod-ngarn table name.
  --help       Show this message and exit.
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
