# mod-ngarn
ModNgarn – Simple job workers

## Usage
### Installation
```
pip install mod-ngarn
psql -d $PGDATABASE -f /usr/local/share/mod-ngarn/Schema/001-initial.blue.sql
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