import os
import signal
import threading
import time
from datetime import timedelta

from django.core.management import call_command
from django.db import transaction, connections, close_old_connections
from django.test import TransactionTestCase, override_settings, mock
from django.utils import timezone

from .exceptions import TaskTransactionError, Retry
from .models import Task, RateLimitRun
from .runners import SimpleRunner


class LockTask(threading.Thread):
    def __init__(self):
        super(LockTask, self).__init__()
        self.locked = threading.Event()
        self.exit = threading.Event()

    def run(self):
        with transaction.atomic():
            Task.objects.next()
            self.locked.set()
            self.exit.wait(timeout=5)
        close_old_connections()


class TaskManagerTest(TransactionTestCase):
    def test_repr(self):
        t1 = Task.objects.create(name='foo')
        self.assertEqual(repr(t1), '<Task foo #{} pending>'.format(t1.pk))

        eta = timezone.now()
        t2 = Task.objects.create(name='bar', eta=eta, status=Task.RETRY)
        self.assertEqual(repr(t2), '<Task bar #{} {} retry>'.format(t2.pk, eta))

    def test_transaction(self):
        with self.assertRaises(TaskTransactionError):
            Task.objects.next()

    def test_locks(self):
        t1 = Task.objects.create(name='foo')
        t2 = Task.objects.create(name='bar')

        l1 = LockTask()

        l2 = LockTask()

        l1.start()
        l1.locked.wait(timeout=5)

        l2.start()

        with transaction.atomic():
            l1.locked.wait(timeout=5)
            l2.locked.wait(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [])

            l1.exit.set()
            l1.join(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1])

            l2.exit.set()
            l2.join(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1, t2])

    def test_eta(self):
        t1 = Task.objects.create(name='foo')
        Task.objects.create(name='foo', eta=timezone.now() + timedelta(minutes=1))

        with transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1])

    def test_retry(self):
        t1 = Task.objects.create(name='foo')
        t1.mark_retry(eta=timezone.now() - timedelta(seconds=1))
        self.assertEqual(t1.status, Task.RETRY)
        self.assertIsNotNone(t1.eta)

        t2 = Task.objects.create(name='foo')
        t2.mark_retry()
        self.assertEqual(t2.status, Task.RETRY)
        self.assertIsNone(t2.eta)

        t3 = Task.objects.create(name='foo')
        t3.mark_retry(delay=timedelta(minutes=2))
        self.assertEqual(t3.status, Task.RETRY)
        self.assertAlmostEqual(
            time.mktime(t3.eta.timetuple()),
            time.mktime((timezone.now() + timedelta(minutes=2)).timetuple()),
            delta=5
        )

        with transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1, t2])

    def test_succeed_and_failed(self):
        for i in range(10):
            Task.objects.create(name='foo')

        with transaction.atomic():
            for idx, task in enumerate(Task.objects.next(limit=10)):
                if idx % 2:
                    task.mark_succeed()
                    self.assertEqual(task.status, Task.SUCCEED)
                else:
                    task.mark_failed()
                    self.assertEqual(task.status, Task.FAILED)

        with transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [])

    def test_log_events(self):
        eta = timezone.now()
        t1 = Task.objects.create(name='foo')
        t1.mark_retry(eta=eta)
        retry_at = t1.updated_at
        t1.mark_succeed()
        self.assertSequenceEqual(t1.events.values_list('status', 'eta', 'created_at'), [
            (Task.PENDING, None, t1.created_at),
            (Task.RETRY, eta, retry_at),
            (Task.SUCCEED, eta, t1.updated_at)
        ])

        self.assertEqual(
            t1.log,
            '{} created\n{} retry\n{} succeed'.format(
                t1.created_at, retry_at, t1.updated_at
            )
        )

        with override_settings(ROBUST_LOG_EVENTS=False):
            t2 = Task.objects.create(name='bar')
            t2.mark_succeed()
            self.assertEqual(t2.log, '')


def test_task():
    pass


TEST_TASK_PATH = '{}.{}'.format(test_task.__module__, test_task.__name__)


class SimpleRunnerTest(TransactionTestCase):
    def test_exec(self):
        task = Task.objects.create(name=TEST_TASK_PATH, payload={'foo': 'bar'})
        runner = SimpleRunner(task)
        with mock.patch(TEST_TASK_PATH) as task_mock:
            runner.run()
            task_mock.assert_has_calls([mock.call(foo='bar')])

    def test_retry(self):
        eta = timezone.now()
        task = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(task)
        with mock.patch(TEST_TASK_PATH, side_effect=Retry(eta=eta)):
            runner.run()
        self.assertEqual(task.status, task.RETRY)
        self.assertEqual(task.eta, eta)

    def test_failed(self):
        task = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(task)
        with mock.patch(TEST_TASK_PATH, side_effect=RuntimeError()):
            runner.run()
        self.assertEqual(task.status, task.FAILED)

    def test_succeed(self):
        task = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(task)
        runner.run()
        self.assertEqual(task.status, task.SUCCEED)


def worker_test_fn(desired_status):
    """
    :type desired_status: int
    """
    if desired_status == Task.RETRY:
        raise Retry()
    elif desired_status == Task.FAILED:
        raise RuntimeError()


TEST_WORKER_TASK_PATH = '{}.{}'.format(worker_test_fn.__module__, worker_test_fn.__name__)


class WorkerTest(TransactionTestCase):
    def test_simple(self):
        Task.objects.create(name=TEST_WORKER_TASK_PATH, payload={'desired_status': Task.SUCCEED})
        Task.objects.create(name=TEST_WORKER_TASK_PATH, payload={'desired_status': Task.FAILED})
        t3 = Task.objects.create(name=TEST_WORKER_TASK_PATH, payload={'desired_status': Task.RETRY})

        call_command('robust_worker', limit=10)

        with transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t3])

    def test_bulk(self):
        for idx in range(100):
            status = Task.SUCCEED if idx % 2 else Task.FAILED
            Task.objects.create(name=TEST_WORKER_TASK_PATH, payload={'desired_status': status})

        call_command('robust_worker', limit=100, bulk=10)

        self.assertEqual(Task.objects.filter(status=Task.SUCCEED).count(), 50)
        self.assertEqual(Task.objects.filter(status=Task.FAILED).count(), 50)

    def _interrupt(self):
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    def test_terminate(self):
        thread = threading.Thread(target=self._interrupt)
        thread.start()
        call_command('robust_worker')
        thread.join()

    def test_recovery(self):
        Task.objects.create(name=TEST_TASK_PATH)
        timeout = object()
        with override_settings(ROBUST_WORKER_FAILURE_TIMEOUT=timeout):
            original_sleep = time.sleep
            with mock.patch('time.sleep', side_effect=lambda _: original_sleep(1)) as sleep_mock:
                call_command('robust_worker', runner='robust.runners.Runner', limit=1)
                sleep_mock.assert_has_calls([mock.call(timeout)])


class TestRateLimit(TransactionTestCase):
    def setUp(self):
        Task.objects.all().delete()
        RateLimitRun.objects.all().delete()

    def test_create(self):
        t1 = Task.objects.create(name=TEST_TASK_PATH, tags=['foo'])
        t2 = Task.objects.create(name=TEST_TASK_PATH, tags=['foo', 'bar'])
        SimpleRunner(t1).run()
        SimpleRunner(t2).run()
        self.assertEqual(RateLimitRun.objects.using('robust_ratelimit').count(), 3)
        self.assertSetEqual(set(RateLimitRun.objects.values_list('tag', flat=True)),
                            {'foo', 'bar'})

    def _run_in_background(self, started, done):
        """
        :type started: threading.Event
        :type done: threading.Event
        """
        try:
            with transaction.atomic():
                task = Task.objects.next(limit=1)[0]
                runner = SimpleRunner(task)
                with mock.patch.object(runner, 'call', new=lambda *args, **kwargs: started.set()):
                    runner.run()
                    done.wait(timeout=5)
        finally:
            close_old_connections()

    def test_detached(self):
        with transaction.atomic():
            Task.objects.create(name=TEST_TASK_PATH, tags=['slow'])

        started = threading.Event()
        done = threading.Event()

        thread = threading.Thread(target=self._run_in_background, args=[started, done])
        thread.start()
        started.wait(timeout=5)

        self.assertEqual(RateLimitRun.objects.count(), 1)

        done.set()
        thread.join()

    def test_limit(self):
        with transaction.atomic():
            runtime = timezone.now()
            RateLimitRun.objects.bulk_create([
                RateLimitRun(tag='foo', created_at=runtime),
                RateLimitRun(tag='bar', created_at=runtime)
            ])
            Task.objects.create(name=TEST_TASK_PATH, tags=['foo'])
            Task.objects.create(name=TEST_TASK_PATH, tags=['foo', 'bar'])
            t1 = Task.objects.create(name=TEST_TASK_PATH, tags=['bar', 'spam'])

            with override_settings(ROBUST_RATE_LIMIT={
                'foo': (1, timedelta(minutes=1)),
                'bar': (10, timedelta(minutes=1))
            }):
                self.assertSequenceEqual(Task.objects.next(limit=10), [t1])
