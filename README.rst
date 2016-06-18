django-robust |Build Status| |Coverage Status| |PyPI Version|
=============================================================

install
-------

.. code:: shell

    $ pip install django-robust

.. code:: python

    INSTALLED_APPS = [
        'robust.apps.RobustConfig',
    ]
    DB = {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
    }
    DATABASES = {
        'default': DB,
        'robust_ratelimit': DB, # <- same db
    }

define tasks
------------

.. code:: python

    from robust import task

    @task()
    def heavy_stuff(foo):
        pass

    @task(tags=['service1'])
    def talk_to_external_service():
        pass

    @task(bind=True, retries=3)
    def retry_me(self):
        self.retry()

schedule tasks
--------------

.. code:: python

    from .tasks import heavy_stuff

    heavy_stuff.delay(foo='bar')

execute tasks
-------------

.. code:: shell

    $ ./manage.py robust_worker

run scheduler
-------------

standalone

.. code:: shell

    $ ./manage.py robust_beat

embedded

.. code:: shell

    $ ./manage.py robust_worker --beat

cleanup
-------

for cleanup completed tasks add ``robust.utils.cleanup`` to robust schedule.

settings
--------

.. code:: python

    ROBUST_RATE_LIMIT = {
        'service1': (1, timedelta(seconds=10)),  # 1/10s,
        'bar':      (20, timedelta(minutes=1)),  # 20/m
    }

    ROBUST_SCHEDULE = [
        (timedelta(seconds=1), 'foo.tasks.every_second'),
        (timedelta(minutes=5), 'foo.tasks.every_5_minutes'),
    ]

    ROBUST_LOG_EVENTS = True  # log all task state changes

    ROBUST_WORKER_FAILURE_TIMEOUT = 5  # wait 5 seconds when worker faces unexpected errors

    ROBUST_NOTIFY_TIMEOUT = 10  # listen to postgres notify for 10 seconds, then poll database

    ROBUST_ALWAYS_EAGER = False  # if this is True, tasks will be executed locally instead of being sent to the queue

    ROBUST_PAYLOAD_PROCESSOR = 'robust.utils.PayloadProcessor'

    ROBUST_SUCCEED_TASK_EXPIRE = datetime.timedelta(hours=1) # succeed tasks cleanup period. Default: 1 hour ago

    ROBUST_FAILED_TASK_EXPIRE = datetime.timedelta(weeks=1) # failed tasks cleanup period. Default: 1 week ago

.. |Build Status| image:: https://travis-ci.org/barbuza/django-robust.svg?branch=master
   :target: https://travis-ci.org/barbuza/django-robust
.. |Coverage Status| image:: https://coveralls.io/repos/github/barbuza/django-robust/badge.svg?branch=master
   :target: https://coveralls.io/github/barbuza/django-robust?branch=master
.. |PyPI Version| image:: https://badge.fury.io/py/django-robust.svg
   :target: https://badge.fury.io/py/django-robust
