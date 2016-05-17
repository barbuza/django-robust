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

.. |Build Status| image:: https://travis-ci.org/barbuza/django-robust.svg?branch=master
   :target: https://travis-ci.org/barbuza/django-robust
.. |Coverage Status| image:: https://coveralls.io/repos/github/barbuza/django-robust/badge.svg?branch=master
   :target: https://coveralls.io/github/barbuza/django-robust?branch=master
