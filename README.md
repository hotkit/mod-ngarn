# mod-ngarn
ModNgarn – Simple job workers

## Usage
### Installation
```
pip install mod-ngarn
```

### Migrate
- Include mod-ngarn schema to SCHEMA_PATH
```
SCHEMA_PATH=$(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")/mod-ngarn/Schema
```
- Run migrate
```
tormor -d $PGDBNAME include $(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")/mod-ngarn/Schema/migrations.txt
```
For more infomation, please check our [Tormor documentation](https://github.com/Proteus-tech/tormor)


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
