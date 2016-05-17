# django-robust [![Build Status](https://travis-ci.org/barbuza/django-robust.svg?branch=master)](https://travis-ci.org/barbuza/django-robust) [![Coverage Status](https://coveralls.io/repos/github/barbuza/django-robust/badge.svg?branch=master)](https://coveralls.io/github/barbuza/django-robust?branch=master)

## install

```shell
$ pip install django-robust
```

```python
INSTALLED_APPS = ('robust', )
DB = {
    'ENGINE': 'django.db.backends.postgresql_psycopg2',
}
DATABASES = {
    'default': DB,
    'robust_ratelimit': DB # <- same db
}
```

## define tasks
```python
from robust import task

@task()
def heavy_stuff(foo):
    pass
```

## schedule tasks
```python
from .tasks import heavy_stuff

heavy_stuff.delay(foo='bar')
```

## execute tasks
```shell
$ ./manage.py robust_worker
```
