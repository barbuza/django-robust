import multiprocessing
import os
import signal
import threading
import time
from datetime import timedelta
from typing import Any, Optional, Tuple, Type, Union, cast
from unittest import mock

import django.core.cache
import django.db
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db.transaction import TransactionManagementError
from django.test import TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from django_redis.cache import RedisCache
from redis import Redis

import robust
from robust import signals
from robust.admin import TaskEventsFilter
from robust.exceptions import Retry, TaskTransactionError
from robust.models import (
    PayloadProcessor,
    Task,
    TaskEvent,
    TaskWrapper,
    cleanup,
    save_tag_run,
    task,
)
from robust.runners import SimpleRunner


def import_path(fn: Union["function", Type["TaskWrapper"]]) -> str:
    return f"{fn.__module__}.{fn.__name__}"


class LockTask(threading.Thread):
    def __init__(self) -> None:
        super(LockTask, self).__init__()
        self.locked = threading.Event()
        self.exit = threading.Event()

    def run(self) -> None:
        with django.db.transaction.atomic():
            Task.objects.next()
            self.locked.set()
            self.exit.wait(timeout=5)
        django.db.close_old_connections()


class TaskManagerTest(TransactionTestCase):
    def test_repr(self) -> None:
        t1 = Task.objects.create(name="foo")
        self.assertEqual(repr(t1), f"<Task foo #{t1.pk} pending>")

        eta = timezone.now()
        t2 = Task.objects.create(name="bar", eta=eta, status=Task.RETRY)
        self.assertEqual(repr(t2), f"<Task bar #{t2.pk} {eta} retry>")

    def test_transaction(self) -> None:
        with self.assertRaises(TaskTransactionError):
            Task.objects.next()

        with self.assertRaises(TransactionManagementError):
            Task.objects.next()

    def test_locks(self) -> None:
        t1 = Task.objects.create(name="foo")
        t2 = Task.objects.create(name="bar")

        l1 = LockTask()

        l2 = LockTask()

        l1.start()
        l1.locked.wait(timeout=5)

        l2.start()

        with django.db.transaction.atomic():
            l1.locked.wait(timeout=5)
            l2.locked.wait(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [])

            l1.exit.set()
            l1.join(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1])

            l2.exit.set()
            l2.join(timeout=5)
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1, t2])

    def test_eta(self) -> None:
        t1 = Task.objects.create(name="foo")
        Task.objects.create(name="foo", eta=timezone.now() + timedelta(minutes=1))

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1])

    def test_retry(self) -> None:
        t1 = Task.objects.create(name="foo")
        t1.mark_retry(eta=timezone.now() - timedelta(seconds=1))
        self.assertEqual(t1.status, Task.RETRY)
        self.assertIsNotNone(t1.eta)

        t2 = Task.objects.create(name="foo")
        t2.mark_retry()
        self.assertEqual(t2.status, Task.RETRY)
        self.assertIsNone(t2.eta)

        t3 = Task.objects.create(name="foo")
        t3.mark_retry(delay=timedelta(minutes=2))
        self.assertEqual(t3.status, Task.RETRY)
        self.assertAlmostEqual(
            time.mktime(t3.eta.timetuple()),
            time.mktime((timezone.now() + timedelta(minutes=2)).timetuple()),
            delta=5,
        )

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1, t2])

    def test_succeed_and_failed(self) -> None:
        for _ in range(10):
            Task.objects.create(name="foo")

        with django.db.transaction.atomic():
            for idx, t in enumerate(Task.objects.next(limit=10)):
                if idx % 2:
                    t.mark_succeed()
                    self.assertEqual(t.status, Task.SUCCEED)
                else:
                    t.mark_failed()
                    self.assertEqual(t.status, Task.FAILED)

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [])

    def test_log_events(self) -> None:
        eta = timezone.now()
        t1 = Task.objects.create(name="foo")
        t1.mark_retry(eta=eta)
        retry_at = t1.updated_at
        t1.mark_succeed()
        self.assertSequenceEqual(
            t1.events.values_list("status", "eta", "created_at"),
            [
                (Task.PENDING, None, t1.created_at),
                (Task.RETRY, eta, retry_at),
                (Task.SUCCEED, eta, t1.updated_at),
            ],
        )

        self.assertEqual(
            t1.log,
            f"{t1.created_at} created\n{retry_at} retry\n{t1.updated_at} succeed",
        )

        with override_settings(ROBUST_LOG_EVENTS=False):
            t2 = Task.objects.create(name="bar")
            t2.mark_succeed()
            self.assertEqual(t2.log, "")


def test_task() -> None:
    pass


TEST_TASK_PATH = import_path(test_task)


class SimpleRunnerTest(TransactionTestCase):
    def _connect_handlers(self) -> Tuple[mock.Mock, mock.Mock, mock.Mock, mock.Mock]:
        started, succeed, failed, retry = (
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        )

        signals.task_started.connect(started)
        signals.task_succeed.connect(succeed)
        signals.task_failed.connect(failed)
        signals.task_retry.connect(retry)

        return started, succeed, failed, retry

    def _assert_signals_called(self, *handlers: mock.Mock) -> None:
        for handler in handlers:
            handler.assert_called_once()
            self.assertGreaterEqual(
                set(handler.call_args[1].keys()), signals.task_signal_args
            )

    def _assert_signals_not_called(self, *handlers: mock.Mock) -> None:
        for handler in handlers:
            handler.assert_not_called()

    def test_exec(self) -> None:
        t = Task.objects.create(name=TEST_TASK_PATH, payload={"foo": "bar"})
        runner = SimpleRunner(t)
        with mock.patch(TEST_TASK_PATH) as task_mock:
            runner.run()
            task_mock.assert_has_calls([mock.call(foo="bar")])

    def test_retry(self) -> None:
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

    def test_failed(self) -> None:
        t = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(t)
        started, succeed, failed, retry = self._connect_handlers()
        with mock.patch(TEST_TASK_PATH, side_effect=RuntimeError()):
            runner.run()
        self.assertEqual(t.status, t.FAILED)
        self._assert_signals_called(started, failed)
        self._assert_signals_not_called(succeed, retry)

    def test_succeed(self) -> None:
        t = Task.objects.create(name=TEST_TASK_PATH)
        runner = SimpleRunner(t)
        started, succeed, failed, retry = self._connect_handlers()
        runner.run()
        self.assertEqual(t.status, t.SUCCEED)
        self._assert_signals_called(started, succeed)
        self._assert_signals_not_called(failed, retry)


def worker_test_fn(desired_status: int) -> None:
    if desired_status == Task.RETRY:
        raise Retry()
    elif desired_status == Task.FAILED:
        raise RuntimeError()


TEST_WORKER_TASK_PATH = import_path(worker_test_fn)


class WorkerTest(TransactionTestCase):
    def test_simple(self) -> None:
        Task.objects.create(
            name=TEST_WORKER_TASK_PATH, payload={"desired_status": Task.SUCCEED}
        )
        Task.objects.create(
            name=TEST_WORKER_TASK_PATH, payload={"desired_status": Task.FAILED}
        )
        t3 = Task.objects.create(
            name=TEST_WORKER_TASK_PATH, payload={"desired_status": Task.RETRY}
        )

        call_command("robust_worker", limit=10)

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t3])

    def test_bulk(self) -> None:
        for idx in range(100):
            status = Task.SUCCEED if idx % 2 else Task.FAILED
            Task.objects.create(
                name=TEST_WORKER_TASK_PATH, payload={"desired_status": status}
            )

        call_command("robust_worker", limit=100, bulk=10)

        self.assertEqual(Task.objects.filter(status=Task.SUCCEED).count(), 50)
        self.assertEqual(Task.objects.filter(status=Task.FAILED).count(), 50)

    def _interrupt(self, pid: int) -> None:
        time.sleep(1)
        os.kill(pid, signal.SIGINT)

    def test_terminate(self) -> None:
        proc = multiprocessing.Process(target=self._interrupt, args=(os.getpid(),))
        proc.start()
        call_command("robust_worker")
        proc.join()

    def test_recovery(self) -> None:
        Task.objects.create(name=TEST_TASK_PATH)
        timeout = object()
        with override_settings(ROBUST_WORKER_FAILURE_TIMEOUT=timeout):
            original_sleep = time.sleep

            def side(_: Any) -> None:
                original_sleep(1)

            with mock.patch("time.sleep", side_effect=side) as sleep_mock:
                call_command("robust_worker", runner="robust.runners.Runner", limit=1)
                sleep_mock.assert_has_calls([mock.call(timeout)])


@override_settings(
    ROBUST_RATE_LIMIT={
        "foo": (1, timedelta(minutes=1)),
        "bar": (10, timedelta(minutes=1)),
    }
)
class RateLimitTest(TransactionTestCase):
    available_apps = ["robust"]

    client: Redis

    def setUp(self) -> None:
        Task.objects.all().delete()
        cache: RedisCache = django.core.cache.caches["robust"]
        self.client = cast(Redis, cache.client.get_client())
        self.client.flushall()

    def test_create(self) -> None:
        t1 = Task.objects.create(name=TEST_TASK_PATH, tags=["foo"])
        t2 = Task.objects.create(name=TEST_TASK_PATH, tags=["foo", "bar"])
        t3 = Task.objects.create(name=TEST_TASK_PATH, tags=["foo", "bar"])
        t4 = Task.objects.create(name=TEST_TASK_PATH, tags=["bar"])
        SimpleRunner(t1).run()
        SimpleRunner(t2).run()
        SimpleRunner(t3).run()
        SimpleRunner(t4).run()

        self.assertEqual(self.client.llen("robust_tag_foo"), 1)
        self.assertEqual(self.client.llen("robust_tag_bar"), 3)

    def _run_in_background(
        self, started: threading.Event, done: threading.Event
    ) -> None:
        try:
            with django.db.transaction.atomic():
                t = Task.objects.next(limit=1)[0]
                runner = SimpleRunner(t)

                def new_(*_: Any, **__: Any) -> None:
                    started.set()

                with mock.patch.object(runner, "call", new=new_):
                    runner.run()
                    done.wait(timeout=5)
        finally:
            django.db.close_old_connections()

    def test_detached(self) -> None:
        with django.db.transaction.atomic():
            Task.objects.create(name=TEST_TASK_PATH, tags=["slow"])

        started = threading.Event()
        done = threading.Event()

        thread = threading.Thread(target=self._run_in_background, args=[started, done])
        with override_settings(ROBUST_RATE_LIMIT={"slow": (1, timedelta(seconds=10))}):
            thread.start()
            started.wait(timeout=5)

        try:
            self.assertEqual(self.client.llen("robust_tag_slow"), 1)
            self.assertEqual(self.client.keys("robust_tag_*"), [b"robust_tag_slow"])

        finally:
            done.set()
            thread.join()

    def test_limit(self) -> None:
        with django.db.transaction.atomic():
            runtime = timezone.now().replace(microsecond=0)
            Task.objects.create(name=TEST_TASK_PATH, tags=["foo", "bar"])
            t1 = Task.objects.create(name=TEST_TASK_PATH, tags=["bar", "spam"])
            Task.objects.create(name=TEST_TASK_PATH, tags=["foo"])
            save_tag_run("foo", runtime)
            save_tag_run("bar", runtime)
            save_tag_run("bar", runtime)

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=2), [t1])

    @override_settings(
        ROBUST_RATE_LIMIT={
            "foo": (2, timedelta(minutes=1)),
            "bar": (10, timedelta(minutes=1)),
            "eggs": (1, timedelta(minutes=1)),
        }
    )
    def test_limit_2(self) -> None:
        with django.db.transaction.atomic():
            runtime = timezone.now().replace(microsecond=0)
            t1 = Task.objects.create(name=TEST_TASK_PATH, tags=["foo", "bar"])
            t2 = Task.objects.create(name=TEST_TASK_PATH, tags=["bar", "spam"])
            Task.objects.create(name=TEST_TASK_PATH, tags=["foo", "eggs"])
            save_tag_run("foo", runtime)
            save_tag_run("bar", runtime)
            save_tag_run("bar", runtime)
            save_tag_run("eggs", runtime)

        with django.db.transaction.atomic():
            self.assertSequenceEqual(Task.objects.next(limit=10), [t1, t2])


@task()
def foo_task(spam: Any = None) -> Any:
    return spam or "bar"


@robust.task(bind=True, tags=["foo", "bar"])
def bound_task(self: TaskWrapper, retry: bool = False) -> TaskWrapper:
    if retry:
        self.retry()
    return self


@task(bind=True, retries=1)
def retry_task(self: TaskWrapper) -> None:
    self.retry()


class TaskDecoratorTest(TransactionTestCase):
    def test_decorator(self) -> None:
        self.assertEqual(foo_task(), "bar")
        self.assertEqual(foo_task(spam="eggs"), "eggs")

        foo_task.delay()
        foo_task.delay(spam="eggs")
        bound_task.delay()
        retry_task.delay()

        with self.assertRaises(bound_task.Retry):
            bound_task(retry=True)

        self.assertTrue(issubclass(cast(type, bound_task()), TaskWrapper))
        self.assertSequenceEqual(bound_task.tags, ["foo", "bar"])

        self.assertEqual(Task.objects.count(), 4)
        self.assertEqual(Task.objects.filter(payload={}).count(), 3)
        self.assertEqual(Task.objects.filter(payload={"spam": "eggs"}).count(), 1)
        self.assertEqual(Task.objects.filter(tags__overlap=["bar", "foo"]).count(), 1)
        self.assertEqual(Task.objects.filter(retries=None).count(), 3)
        self.assertEqual(Task.objects.filter(retries=1).count(), 1)


# noinspection PyUnusedLocal
@task()
def args_task(a: Any, b: Any, c: Any) -> None:
    pass


# noinspection PyUnusedLocal
@task(bind=True)
def bound_args_task(self: TaskWrapper, a: Any, b: Any, c: Any) -> None:
    pass


class ArgsTest(TransactionTestCase):
    def test_unbound(self) -> None:
        args_task(1, 2, 3)

        self.assertDictEqual(args_task.delay().payload, {})
        self.assertDictEqual(args_task.delay(1).payload, {"a": 1})
        self.assertDictEqual(args_task.delay(1, 2).payload, {"a": 1, "b": 2})
        self.assertDictEqual(args_task.delay(1, 2, 3).payload, {"a": 1, "b": 2, "c": 3})
        self.assertDictEqual(
            args_task.delay(1, 2, c=3).payload, {"a": 1, "b": 2, "c": 3}
        )

        with self.assertRaises(TypeError):
            args_task.delay(1, 2, 3, 4)

        with self.assertRaises(TypeError):
            args_task.delay(1, a=1)

    def test_bound(self) -> None:
        bound_args_task(1, 2, 3)

        self.assertDictEqual(bound_args_task.delay().payload, {})
        self.assertDictEqual(bound_args_task.delay(1).payload, {"a": 1})
        self.assertDictEqual(bound_args_task.delay(1, 2).payload, {"a": 1, "b": 2})
        self.assertDictEqual(
            bound_args_task.delay(1, 2, 3).payload, {"a": 1, "b": 2, "c": 3}
        )
        self.assertDictEqual(
            bound_args_task.delay(1, 2, c=3).payload, {"a": 1, "b": 2, "c": 3}
        )

        with self.assertRaises(TypeError):
            bound_args_task.delay(1, 2, 3, 4)

        with self.assertRaises(TypeError):
            bound_args_task.delay(1, a=1)


class EagerModeTest(TransactionTestCase):
    def test_eager_mode(self) -> None:
        with override_settings(ROBUST_ALWAYS_EAGER=True):
            self.assertEqual(foo_task.delay(), "bar")
            self.assertEqual(foo_task.delay(spam="eggs"), "eggs")

            with self.assertRaises(bound_task.Retry):
                bound_task.delay(retry=True)

            with self.assertRaises(retry_task.Retry):
                retry_task.delay()

            self.assertFalse(Task.objects.count())

    def test_kwargs_non_json_serializable(self) -> None:
        with override_settings(ROBUST_ALWAYS_EAGER=True):
            with self.assertRaises(TypeError):
                foo_task.delay(spam=timezone.now())

        with self.assertRaises(TypeError):
            foo_task.delay(span=timezone.now())


@override_settings(
    ROBUST_ALWAYS_EAGER=True,
    ROBUST_SUCCEED_TASK_EXPIRE=timedelta(hours=1),
    ROBUST_FAILED_TASK_EXPIRE=timedelta(weeks=1),
)
class CleanupTest(TransactionTestCase):
    def _create_task(
        self, name: str, status: int, updated_timedelta: Optional[timedelta] = None
    ) -> Task:
        t = Task.objects.create(name=name, status=status)
        if updated_timedelta:
            self._set_update_at(t.pk, updated_timedelta)
        return t

    def _set_update_at(self, task_id: int, updated_timedelta: timedelta) -> None:
        Task.objects.filter(pk=task_id).update(
            updated_at=timezone.now() - updated_timedelta
        )

    def test_succeed(self) -> None:
        t = self._create_task("test_succeed", Task.SUCCEED)

        self.assertEqual(Task.objects.count(), 1)
        cleanup()
        self.assertEqual(Task.objects.count(), 1)

        self._set_update_at(t.pk, timedelta(hours=2))
        cleanup()
        self.assertEqual(Task.objects.count(), 0)

    def test_succeed_with_fails(self) -> None:
        t1 = self._create_task("test_succeed", Task.SUCCEED, timedelta(hours=2))
        TaskEvent.objects.create(task=t1, status=Task.RETRY, created_at=timezone.now())

        t2 = self._create_task("test_succeed", Task.SUCCEED, timedelta(hours=2))
        TaskEvent.objects.create(task=t2, status=Task.FAILED, created_at=timezone.now())

        self.assertEqual(Task.objects.count(), 2)
        cleanup()
        self.assertEqual(Task.objects.count(), 2)

        self._set_update_at(t1.pk, timedelta(weeks=2))
        self._set_update_at(t2.pk, timedelta(weeks=2))
        cleanup()

        self.assertEqual(Task.objects.count(), 0)

    def test_expired(self) -> None:
        tasks = []
        for status, name in Task.STATUS_CHOICES:
            tasks.append(self._create_task(name, status))

        t = self._create_task("test_succeed_with_fails", Task.SUCCEED)
        TaskEvent.objects.create(task=t, status=Task.FAILED, created_at=timezone.now())
        tasks.append(t)

        self.assertEqual(Task.objects.count(), 5)
        cleanup()
        self.assertEqual(Task.objects.count(), 5)

        Task.objects.all().update(updated_at=timezone.now() - timedelta(weeks=2))
        cleanup()

        self.assertFalse(
            Task.objects.filter(status__in={Task.SUCCEED, Task.FAILED}).exists()
        )


class AdminTest(TransactionTestCase):
    def setUp(self) -> None:
        username, password = "username", "password"
        self.admin = User.objects.create_superuser(username, "email@host.com", password)
        self.client.login(username=username, password=password)
        self.list_url = reverse("admin:robust_task_changelist")
        self.t1 = Task.objects.create(name=TEST_TASK_PATH)
        self.t2 = Task.objects.create(name=TEST_WORKER_TASK_PATH, status=Task.RETRY)

    def task_url(self, obj: Task) -> str:
        return reverse("admin:robust_task_change", args=(obj.pk,))

    def test_installed(self) -> None:
        response = self.client.get("/admin/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(self.list_url, response.content.decode("utf-8"))

    def test_list(self) -> None:
        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, 200)

        content = response.content.decode("utf-8")
        self.assertIn(self.task_url(self.t1), content)
        self.assertIn(self.task_url(self.t2), content)

    def test_filters(self) -> None:
        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, 200)

        content = response.content.decode("utf-8")
        self.assertIn(f"?status__exact={Task.PENDING}", content)
        self.assertIn(f"?status__exact={Task.FAILED}", content)
        self.assertIn(f"?status__exact={Task.SUCCEED}", content)
        self.assertIn(f"?status__exact={Task.RETRY}", content)
        self.assertIn(f"?events={TaskEventsFilter.SUCCEED}", content)
        self.assertIn(f"?events={TaskEventsFilter.TROUBLED}", content)

        response = self.client.get(self.list_url + f"?status__exact={Task.PENDING}")
        self.assertEqual(response.status_code, 200)

        content = response.content.decode("utf-8")
        self.assertIn(self.task_url(self.t1), content)
        self.assertNotIn(self.task_url(self.t2), content)

        response = self.client.get(self.list_url + f"?status__exact={Task.RETRY}")
        self.assertEqual(response.status_code, 200)

        content = response.content.decode("utf-8")
        self.assertNotIn(self.task_url(self.t1), content)
        self.assertIn(self.task_url(self.t2), content)

        response = self.client.get(
            self.list_url + f"?events={TaskEventsFilter.SUCCEED}"
        )
        self.assertEqual(response.status_code, 200)

        response = self.client.get(
            self.list_url + f"?events={TaskEventsFilter.TROUBLED}"
        )
        self.assertEqual(response.status_code, 200)

    def test_details_page(self) -> None:
        response = self.client.get(self.task_url(self.t1))
        self.assertEqual(response.status_code, 200)

        content = response.content.decode("utf-8")
        self.assertIn(self.t1.name, content)
        self.assertIn(self.t1.get_status_display(), content)
        self.assertEqual(content.count("input"), 1)

        retry_url = reverse("admin:robust_task_actions", args=(self.t1.pk, "retry"))
        self.assertIn(retry_url, content)

    def test_retry(self) -> None:
        self.t1.mark_failed()
        response = self.client.get(
            reverse("admin:robust_task_actions", args=(self.t1.pk, "retry"))
        )
        self.assertEqual(response.status_code, 302)
        self.t1.refresh_from_db()
        self.assertEqual(self.t1.status, Task.RETRY)


@task()
def every_second() -> None:
    pass


@task()
def every_2_seconds() -> None:
    pass


@override_settings(
    ROBUST_SCHEDULE=[
        (timedelta(seconds=1), import_path(every_second)),
        (timedelta(seconds=2), import_path(every_2_seconds)),
    ]
)
class TestBeat(TransactionTestCase):
    def test_invalid(self) -> None:
        with override_settings(ROBUST_SCHEDULE=None):
            with self.assertRaises(RuntimeError):
                call_command("robust_beat")

        with override_settings(
            ROBUST_SCHEDULE=[(timedelta(seconds=1), "foobar_spameggs")]
        ):
            with self.assertRaises(ImportError):
                call_command("robust_beat")

        with override_settings(ROBUST_SCHEDULE=[(timedelta(seconds=1), "os.getpid")]):
            with self.assertRaises(RuntimeError):
                call_command("robust_beat")

    def _interrupt(self, pid: int) -> None:
        time.sleep(4.5)
        os.kill(pid, signal.SIGINT)

    def test_standalone(self) -> None:
        proc = multiprocessing.Process(target=self._interrupt, args=(os.getpid(),))
        proc.start()
        call_command("robust_beat")
        proc.join()

        self.assertEqual(Task.objects.filter(name=import_path(every_second)).count(), 4)
        self.assertEqual(
            Task.objects.filter(name=import_path(every_2_seconds)).count(), 2
        )

    def test_embedded(self) -> None:
        call_command("robust_worker", limit=6, beat=True)

        self.assertEqual(Task.objects.filter(name=import_path(every_second)).count(), 4)
        self.assertEqual(
            Task.objects.filter(name=import_path(every_2_seconds)).count(), 2
        )


class FakeProcessor(PayloadProcessor):
    @staticmethod
    def wrap_payload(payload: dict) -> dict:
        if "fail" in payload:
            raise RuntimeError("fail")
        return dict(payload, fake="processor")

    @staticmethod
    def unwrap_payload(payload: dict) -> dict:
        payload = dict(payload)
        del payload["fake"]
        return payload


@override_settings(ROBUST_PAYLOAD_PROCESSOR="robust.tests.test_basic.FakeProcessor")
class TestPayloadProcessor(TransactionTestCase):
    def test_wrap(self) -> None:
        foo_task.delay()
        self.assertEqual(Task.objects.filter(payload={"fake": "processor"}).count(), 1)

    def test_unwrap(self) -> None:
        instance = foo_task.delay()
        with mock.patch(import_path(foo_task)) as task_mock:
            SimpleRunner(instance).run()
            self.assertListEqual(task_mock.mock_calls, [mock.call()])

    def test_fail(self) -> None:
        with self.assertRaises(RuntimeError, msg="fail"):
            foo_task.delay(fail=True)

        with override_settings(ROBUST_ALWAYS_EAGER=True):
            with self.assertRaises(RuntimeError, msg="fail"):
                foo_task.delay(fail=True)


@task()
def failing_task() -> None:
    raise RuntimeError("spam")


@task(bind=True)
def bound_failing_task(self: TaskWrapper) -> None:
    try:
        raise RuntimeError("bar")
    except RuntimeError:
        self.retry()


class TracebackTest(TransactionTestCase):
    def test_empty(self) -> None:
        instance = bound_task.delay()
        SimpleRunner(instance).run()
        self.assertIsNone(instance.traceback)
        self.assertIsNone(instance.events.get(status=Task.SUCCEED).traceback)

        instance = bound_task.delay(retry=True)
        SimpleRunner(instance).run()
        self.assertIsNone(instance.traceback)

    def test_trace(self) -> None:
        instance = failing_task.delay()
        SimpleRunner(instance).run()
        self.assertIn("failing_task", instance.traceback)
        self.assertIn("RuntimeError", instance.traceback)
        self.assertIn("spam", instance.traceback)

        instance = bound_failing_task.delay()
        SimpleRunner(instance).run()
        self.assertIn("bound_failing_task", instance.traceback)
        self.assertIn("RuntimeError", instance.traceback)
        self.assertIn("bar", instance.traceback)


class TaskKwargsTest(TransactionTestCase):
    def test_eta(self) -> None:
        eta = timezone.now() + timedelta(hours=1)
        t = retry_task.with_task_kwargs(eta=eta).delay(foo="bar")
        self.assertEqual(eta, t.eta)
        self.assertDictEqual({"foo": "bar"}, t.payload)

    def test_delay(self) -> None:
        t = retry_task.with_task_kwargs(delay=timedelta(hours=1)).delay(foo="bar")
        self.assertAlmostEqual(
            time.mktime((timezone.now() + timedelta(hours=1)).timetuple()),
            time.mktime(t.eta.timetuple()),
            delta=5,
        )
        self.assertDictEqual({"foo": "bar"}, t.payload)

    def test_both(self) -> None:
        with self.assertRaises(RuntimeError):
            retry_task.with_task_kwargs(eta=timezone.now(), delay=timedelta(hours=1))


class NotifyTest(TransactionTestCase):
    def test_notify(self) -> None:
        with mock.patch("robust.receivers._notify_change") as notify_mock:
            foo_task.delay()
            foo_task.delay()
        notify_mock.assert_has_calls([notify_mock(), notify_mock()])

    def test_notify_once_per_transaction(self) -> None:
        with django.db.transaction.atomic():
            with mock.patch("robust.receivers._notify_change") as notify_mock:
                foo_task.delay()
                foo_task.delay()
                foo_task.delay()
        notify_mock.assert_called_once()


class DedupeTest(TransactionTestCase):
    def test_dedupe_no_transaction(self) -> None:
        foo_task.delay(a=1)
        foo_task.delay(a=1)
        foo_task.delay(a=2)
        self.assertEqual(Task.objects.count(), 3)

    def test_dedupe(self) -> None:
        with django.db.transaction.atomic():
            foo_task.delay(a=1)
            foo_task.delay(a=1)
            foo_task.delay(a=2)
        self.assertEqual(Task.objects.count(), 2)
