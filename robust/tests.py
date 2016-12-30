import multiprocessing
import os
import signal
import threading
import time
from datetime import timedelta, datetime

from django.contrib.auth.models import User
from django.core.management import call_command
from django.core.urlresolvers import reverse
from django.db import transaction, close_old_connections
from django.test import TransactionTestCase, override_settings, mock
from django.utils import timezone

from .exceptions import TaskTransactionError, Retry
from .models import Task, RateLimitRun, TaskEvent
from .runners import SimpleRunner
from .utils import task, TaskWrapper, PayloadProcessor, cleanup
from .admin import TaskEventsFilter


def import_path(fn):
    """
    :type fn: Any
    """
    return '{}.{}'.format(fn.__module__, fn.__name__)


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
            for idx, t in enumerate(Task.objects.next(limit=10)):
                if idx % 2:
                    t.mark_succeed()
                    self.assertEqual(t.status, Task.SUCCEED)
                else:
                    t.mark_failed()
                    self.assertEqual(t.status, Task.FAILED)

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


TEST_TASK_PATH = import_path(test_task)


class SimpleRunnerTest(TransactionTestCase):
    def test_exec(self):
        t = Task.objects.create(name=TEST_TASK_PATH, payload={'foo': 'bar'})
        runner = SimpleRunner(t)
        with mock.patch(TEST_TASK_PATH) as task_mock:
            runner.run()
            task_mock.assert_has_calls([mock.call(foo='bar')])

    def test_retry(self):
        eta = timezone.now()
        t = Task.objects.create(name=TEST_TASK_PATH, retries=2)
        runner = SimpleRunner(t)
        with mock.patch(TEST_TASK_PATH, side_effect=Retry(eta=eta)):
            runner.run()
            self.assertEqual(t.status, t.RETRY)
            self.assertEqual(t.eta, eta)
            self.assertEqual(t.retries, 1)
            runner.run()
            self.assertEqual(t.status, t.RETRY)
            self.assertEqual(t.eta, eta)
            self.assertEqual(t.retries, 0)
            runner.run()
            self.assertEqual(t.status, t.FAILED)

    def test_failed(self):
        t = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(t)
        with mock.patch(TEST_TASK_PATH, side_effect=RuntimeError()):
            runner.run()
        self.assertEqual(t.status, t.FAILED)

    def test_succeed(self):
        t = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(t)
        runner.run()
        self.assertEqual(t.status, t.SUCCEED)


def worker_test_fn(desired_status):
    """
    :type desired_status: int
    """
    if desired_status == Task.RETRY:
        raise Retry()
    elif desired_status == Task.FAILED:
        raise RuntimeError()


TEST_WORKER_TASK_PATH = import_path(worker_test_fn)


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

    def _interrupt(self, pid):
        time.sleep(1)
        os.kill(pid, signal.SIGINT)

    def test_terminate(self):
        proc = multiprocessing.Process(target=self._interrupt, args=(os.getpid(),))
        proc.start()
        call_command('robust_worker')
        proc.join()

    def test_recovery(self):
        Task.objects.create(name=TEST_TASK_PATH)
        timeout = object()
        with override_settings(ROBUST_WORKER_FAILURE_TIMEOUT=timeout):
            original_sleep = time.sleep
            with mock.patch('time.sleep', side_effect=lambda _: original_sleep(1)) as sleep_mock:
                call_command('robust_worker', runner='robust.runners.Runner', limit=1)
                sleep_mock.assert_has_calls([mock.call(timeout)])


@override_settings(ROBUST_RATE_LIMIT={
    'foo': (1, timedelta(minutes=1)),
    'bar': (10, timedelta(minutes=1))
})
class RateLimitTest(TransactionTestCase):
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
                t = Task.objects.next(limit=1)[0]
                runner = SimpleRunner(t)
                # noinspection PyUnresolvedReferences
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
            Task.objects.create(name=TEST_TASK_PATH, tags=['foo', 'bar'])
            RateLimitRun.objects.bulk_create([
                RateLimitRun(tag='foo', created_at=runtime),
                RateLimitRun(tag='bar', created_at=runtime),
                RateLimitRun(tag='bar', created_at=runtime)
            ])
            t1 = Task.objects.create(name=TEST_TASK_PATH, tags=['bar', 'spam'])
            t2 = Task.objects.create(name=TEST_TASK_PATH, tags=['foo'])

        with transaction.atomic():
            with override_settings(ROBUST_RATE_LIMIT={
                'foo': (1, timedelta(minutes=1)),
                'bar': (10, timedelta(minutes=1))
            }):
                self.assertSequenceEqual(Task.objects.next(limit=10), [t1])

        with transaction.atomic():
            with override_settings(ROBUST_RATE_LIMIT={
                'foo': (3, timedelta(minutes=1)),
                'bar': (2, timedelta(minutes=1))
            }):
                self.assertSequenceEqual(Task.objects.next(limit=10), [t2])


@task()
def foo_task(spam=None):
    return spam or 'bar'


@task(bind=True, tags=['foo', 'bar'])
def bound_task(self, retry=False):
    if retry:
        self.retry()
    return self


@task(bind=True, retries=1)
def retry_task(self):
    self.retry()


class TaskDecoratorTest(TransactionTestCase):
    def test_decorator(self):
        self.assertEqual(foo_task(), 'bar')
        self.assertEqual(foo_task(spam='eggs'), 'eggs')

        foo_task.delay()
        foo_task.delay(spam='eggs')
        bound_task.delay()
        retry_task.delay()

        with self.assertRaises(bound_task.Retry):
            bound_task(retry=True)

        self.assertTrue(issubclass(bound_task(), TaskWrapper))
        self.assertSequenceEqual(bound_task.tags, ['foo', 'bar'])

        self.assertEqual(Task.objects.count(), 4)
        self.assertEqual(Task.objects.filter(payload={}).count(), 3)
        self.assertEqual(Task.objects.filter(payload={'spam': 'eggs'}).count(), 1)
        self.assertEqual(Task.objects.filter(tags__overlap=['bar', 'foo']).count(), 1)
        self.assertEqual(Task.objects.filter(retries=None).count(), 3)
        self.assertEqual(Task.objects.filter(retries=1).count(), 1)


@task()
def args_task(a, b, c):
    pass


@task(bind=True)
def bound_args_task(self, a, b, c):
    pass


class ArgsTest(TransactionTestCase):
    def test_unbound(self):
        args_task(1, 2, 3)

        self.assertDictEqual(args_task.delay().payload, {})
        self.assertDictEqual(args_task.delay(1).payload, {'a': 1})
        self.assertDictEqual(args_task.delay(1, 2).payload, {'a': 1, 'b': 2})
        self.assertDictEqual(args_task.delay(1, 2, 3).payload, {'a': 1, 'b': 2, 'c': 3})
        self.assertDictEqual(args_task.delay(1, 2, c=3).payload, {'a': 1, 'b': 2, 'c': 3})

        with self.assertRaises(TypeError):
            args_task.delay(1, 2, 3, 4)

        with self.assertRaises(TypeError):
            args_task.delay(1, a=1)

    def test_bound(self):
        bound_args_task(1, 2, 3)

        self.assertDictEqual(bound_args_task.delay().payload, {})
        self.assertDictEqual(bound_args_task.delay(1).payload, {'a': 1})
        self.assertDictEqual(bound_args_task.delay(1, 2).payload, {'a': 1, 'b': 2})
        self.assertDictEqual(bound_args_task.delay(1, 2, 3).payload, {'a': 1, 'b': 2, 'c': 3})
        self.assertDictEqual(bound_args_task.delay(1, 2, c=3).payload, {'a': 1, 'b': 2, 'c': 3})

        with self.assertRaises(TypeError):
            bound_args_task.delay(1, 2, 3, 4)

        with self.assertRaises(TypeError):
            bound_args_task.delay(1, a=1)


class EagerModeTest(TransactionTestCase):
    def test_eager_mode(self):
        with override_settings(ROBUST_ALWAYS_EAGER=True):
            self.assertEqual(foo_task.delay(), 'bar')
            self.assertEqual(foo_task.delay(spam='eggs'), 'eggs')

            with self.assertRaises(bound_task.Retry):
                bound_task.delay(retry=True)

            with self.assertRaises(retry_task.Retry):
                retry_task.delay()

            self.assertFalse(Task.objects.count())

    def test_kwargs_non_json_serializable(self):
        with override_settings(ROBUST_ALWAYS_EAGER=True):
            with self.assertRaises(TypeError):
                foo_task.delay(spam=datetime.now())

        with self.assertRaises(TypeError):
            foo_task.delay(span=datetime.now())


@override_settings(
    ROBUST_ALWAYS_EAGER=True,
    ROBUST_SUCCEED_TASK_EXPIRE=timedelta(hours=1),
    ROBUST_FAILED_TASK_EXPIRE=timedelta(weeks=1),
)
class CleanupTest(TransactionTestCase):
    def setUp(self):
        self.now = datetime.now()

    def tearDown(self):
        Task.objects.all().delete()
        super(CleanupTest, self).tearDown()

    def _create_task(self, name, status, updated_timedelta=None):
        task = Task.objects.create(name=name, status=status)
        if updated_timedelta:
            self._set_update_at(task.pk, updated_timedelta)
        return task

    def _set_update_at(self, task_id, updated_timedelta):
        Task.objects.filter(pk=task_id).update(updated_at=self.now - updated_timedelta)

    def test_succeed(self):
        task = self._create_task('test_succeed', Task.SUCCEED)

        self.assertEqual(Task.objects.count(), 1)
        cleanup()
        self.assertEqual(Task.objects.count(), 1)

        self._set_update_at(task.pk, timedelta(hours=1, seconds=10))
        cleanup()
        self.assertEqual(Task.objects.count(), 0)

    def test_succeed_with_fails(self):
        task = self._create_task('test_succeed', Task.SUCCEED, timedelta(hours=1, seconds=10))
        TaskEvent.objects.create(task=task, status=Task.RETRY, created_at=self.now)

        task2 = self._create_task('test_succeed', Task.SUCCEED, timedelta(hours=1, seconds=10))
        TaskEvent.objects.create(task=task2, status=Task.FAILED, created_at=self.now)

        self.assertEqual(Task.objects.count(), 2)
        cleanup()
        self.assertEqual(Task.objects.count(), 2)

        self._set_update_at(task.pk, timedelta(weeks=1, seconds=10))
        self._set_update_at(task2.pk, timedelta(weeks=1, seconds=10))
        cleanup()

        self.assertEqual(Task.objects.count(), 0)

    def test_expired(self):
        tasks = []
        for status, name in Task.STATUS_CHOICES:
            tasks.append(self._create_task(name, status))

        task = self._create_task('test_succeed_with_fails', Task.SUCCEED)
        TaskEvent.objects.create(task=task, status=Task.FAILED, created_at=self.now)
        tasks.append(task)

        self.assertEqual(Task.objects.count(), 5)
        cleanup()
        self.assertEqual(Task.objects.count(), 5)

        Task.objects.all().update(updated_at=self.now - timedelta(weeks=1, seconds=10))
        cleanup()
        self.assertEqual(Task.objects.count(), 0)


class AdminTest(TransactionTestCase):
    def setUp(self):
        username, password = 'username', 'password'
        self.admin = User.objects.create_superuser(username, 'email@host.com', password)
        self.client.login(username=username, password=password)
        self.list_url = reverse('admin:robust_task_changelist')
        self.t1 = Task.objects.create(name=TEST_TASK_PATH)
        self.t2 = Task.objects.create(name=TEST_WORKER_TASK_PATH, status=Task.RETRY)

    def task_url(self, obj):
        """
        :type obj: Task
        :rtype str
        """
        return reverse('admin:robust_task_change', args=(obj.pk,))

    def test_installed(self):
        response = self.client.get('/admin/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(self.list_url, response.content.decode('utf-8'))

    def test_list(self):
        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, 200)

        content = response.content.decode('utf-8')
        self.assertIn(self.task_url(self.t1), content)
        self.assertIn(self.task_url(self.t2), content)

    def test_filters(self):
        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, 200)

        content = response.content.decode('utf-8')
        self.assertIn('?status__exact={}'.format(Task.PENDING), content)
        self.assertIn('?status__exact={}'.format(Task.FAILED), content)
        self.assertIn('?status__exact={}'.format(Task.SUCCEED), content)
        self.assertIn('?status__exact={}'.format(Task.RETRY), content)
        self.assertIn('?events={}'.format(TaskEventsFilter.SUCCEED), content)
        self.assertIn('?events={}'.format(TaskEventsFilter.TROUBLED), content)

        response = self.client.get(self.list_url + '?status__exact={}'.format(Task.PENDING))
        self.assertEqual(response.status_code, 200)

        content = response.content.decode('utf-8')
        self.assertIn(self.task_url(self.t1), content)
        self.assertNotIn(self.task_url(self.t2), content)

        response = self.client.get(self.list_url + '?status__exact={}'.format(Task.RETRY))
        self.assertEqual(response.status_code, 200)

        content = response.content.decode('utf-8')
        self.assertNotIn(self.task_url(self.t1), content)
        self.assertIn(self.task_url(self.t2), content)

        response = self.client.get(self.list_url + '?events={}'.format(TaskEventsFilter.SUCCEED))
        self.assertEqual(response.status_code, 200)

        response = self.client.get(self.list_url + '?events={}'.format(TaskEventsFilter.TROUBLED))
        self.assertEqual(response.status_code, 200)

    def test_details_page(self):
        response = self.client.get(self.task_url(self.t1))
        self.assertEqual(response.status_code, 200)

        content = response.content.decode('utf-8')
        self.assertIn(self.t1.name, content)
        self.assertIn(self.t1.get_status_display(), content)
        self.assertEqual(content.count('input'), 1)

        retry_url = reverse('admin:robust_task_actions', args=(self.t1.pk, 'retry'))
        self.assertIn(retry_url, content)

    def test_retry(self):
        self.t1.mark_failed()
        response = self.client.get(reverse('admin:robust_task_actions', args=(self.t1.pk, 'retry')))
        self.assertEqual(response.status_code, 302)
        self.t1.refresh_from_db()
        self.assertEqual(self.t1.status, Task.RETRY)


@task()
def every_second():
    pass


@task()
def every_2_seconds():
    pass


@override_settings(ROBUST_SCHEDULE=[
    (timedelta(seconds=1), import_path(every_second)),
    (timedelta(seconds=2), import_path(every_2_seconds))
])
class TestBeat(TransactionTestCase):
    def test_invalid(self):
        with override_settings(ROBUST_SCHEDULE=None):
            with self.assertRaises(RuntimeError):
                call_command('robust_beat')

        with override_settings(ROBUST_SCHEDULE=[
            (timedelta(seconds=1), 'foobar_spameggs')
        ]):
            with self.assertRaises(ImportError):
                call_command('robust_beat')

        with override_settings(ROBUST_SCHEDULE=[
            (timedelta(seconds=1), 'os.getpid')
        ]):
            with self.assertRaises(RuntimeError):
                call_command('robust_beat')

    def _interrupt(self, pid):
        time.sleep(4.5)
        os.kill(pid, signal.SIGINT)

    def test_standalone(self):
        proc = multiprocessing.Process(target=self._interrupt, args=(os.getpid(),))
        proc.start()
        call_command('robust_beat')
        proc.join()

        self.assertEqual(Task.objects.filter(name=import_path(every_second)).count(), 4)
        self.assertEqual(Task.objects.filter(name=import_path(every_2_seconds)).count(), 2)

    def test_embedded(self):
        call_command('robust_worker', limit=6, beat=True)

        self.assertEqual(Task.objects.filter(name=import_path(every_second)).count(), 4)
        self.assertEqual(Task.objects.filter(name=import_path(every_2_seconds)).count(), 2)


class FakeProcessor(PayloadProcessor):
    @staticmethod
    def wrap_payload(payload):
        if 'fail' in payload:
            raise RuntimeError('fail')
        return dict(payload, fake='processor')

    @staticmethod
    def unwrap_payload(payload):
        payload = dict(payload)
        del payload['fake']
        return payload


@override_settings(ROBUST_PAYLOAD_PROCESSOR='robust.tests.FakeProcessor')
class TestPayloadProcessor(TransactionTestCase):
    def test_wrap(self):
        foo_task.delay()
        self.assertEqual(Task.objects.filter(payload={'fake': 'processor'}).count(), 1)

    def test_unwrap(self):
        instance = foo_task.delay()
        with mock.patch(import_path(foo_task)) as task_mock:
            SimpleRunner(instance).run()
            self.assertListEqual(task_mock.mock_calls, [mock.call()])

    def test_fail(self):
        with self.assertRaises(RuntimeError, msg='fail'):
            foo_task.delay(fail=True)

        with override_settings(ROBUST_ALWAYS_EAGER=True):
            with self.assertRaises(RuntimeError, msg='fail'):
                foo_task.delay(fail=True)


@task()
def failing_task():
    raise RuntimeError('spam')


@task(bind=True)
def bound_failing_task(self):
    try:
        raise RuntimeError('bar')
    except RuntimeError:
        self.retry()


class TracebackTest(TransactionTestCase):
    def test_empty(self):
        instance = bound_task.delay()
        SimpleRunner(instance).run()
        self.assertIsNone(instance.traceback)
        self.assertIsNone(instance.events.get(status=Task.SUCCEED).traceback)

        instance = bound_task.delay(retry=True)
        SimpleRunner(instance).run()
        self.assertIsNone(instance.traceback)

    def test_trace(self):
        instance = failing_task.delay()
        SimpleRunner(instance).run()
        self.assertIn('failing_task', instance.traceback)
        self.assertIn('RuntimeError', instance.traceback)
        self.assertIn('spam', instance.traceback)

        instance = bound_failing_task.delay()
        SimpleRunner(instance).run()
        self.assertIn('bound_failing_task', instance.traceback)
        self.assertIn('RuntimeError', instance.traceback)
        self.assertIn('bar', instance.traceback)
