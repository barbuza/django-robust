django-robust |Build Status| |Coverage Status|
==============================================

install
-------

.. code:: shell

    $ pip install django-robust

.. code:: python

    INSTALLED_APPS = ('robust', )
    DB = {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
    }
    DATABASES = {
        'default': DB,
        'robust_ratelimit': DB # <- same db
    }

define tasks
------------

.. code:: python

    from robust import task

    @task()
    def heavy_stuff(foo):
        pass

schedule tasks
--------------

.. code:: python

    from .tasks import heavy_stuff

    heavy_stuff.delay(foo='bar')

execute tasks
-------------

.. code:: shell

    $ ./manage.py robust_worker

settings
--------

.. code:: python

    ROBUST_RATE_LIMIT = {
        'foo': (1, timedelta(seconds=10)),  # 1/10s,
        'bar': (20, timedelta(minutes=1)),  # 20/m
    }

    ROBUST_LOG_EVENTS = True  # log all task state changes

    ROBUST_WORKER_FAILURE_TIMEOUT = 5  # wait 5 seconds when worker faces unexpected errors

    ROBUST_NOTIFY_TIMEOUT = 10  # listen to postgres notify for 10 seconds, then poll database

.. |Build Status| image:: https://travis-ci.org/barbuza/django-robust.svg?branch=master
   :target: https://travis-ci.org/barbuza/django-robust
.. |Coverage Status| image:: https://coveralls.io/repos/github/barbuza/django-robust/badge.svg?branch=master
   :target: https://coveralls.io/github/barbuza/django-robust?branch=master