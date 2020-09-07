from datetime import timedelta
from typing import cast

from django.core.cache import caches
from django.test import SimpleTestCase, override_settings
from django.utils import timezone
from django_redis.cache import RedisCache
from redis import Redis

from ..models import calc_tags_eta, save_tag_run


@override_settings(
    ROBUST_RATE_LIMIT={
        "foo": (1, timedelta(seconds=10)),
        "bar": (3, timedelta(minutes=1)),
        "spam": (2, timedelta(minutes=5)),
        "eggs": (1, timedelta(days=1)),
    }
)
class TagsTestCase(SimpleTestCase):
    client: Redis

    def setUp(self) -> None:
        cache: RedisCache = caches["robust"]
        self.client = cast(Redis, cache.client.get_client())
        self.client.flushall()

    @override_settings(ROBUST_RATE_LIMIT=None)
    def test_absent(self) -> None:
        self.assertDictEqual(calc_tags_eta(["foo", "bar"]), {})

    def test_save_unknown(self) -> None:
        save_tag_run("unknown", timezone.now().replace(microsecond=0))
        self.assertListEqual(self.client.keys("*"), [])

    def test_calc(self) -> None:
        start = timezone.now().replace(microsecond=0)
        save_tag_run("foo", start)

        save_tag_run("bar", start)
        save_tag_run("bar", start + timedelta(seconds=5))
        save_tag_run("bar", start + timedelta(seconds=10))

        save_tag_run("spam", start)
        save_tag_run("spam", start + timedelta(minutes=1))
        save_tag_run("spam", start + timedelta(minutes=2))

        self.assertDictEqual(
            calc_tags_eta(["foo", "bar", "spam", "eggs"]),
            {
                "foo": start + timedelta(seconds=10),
                "bar": start + timedelta(minutes=1),
                "spam": start + timedelta(minutes=6),
            },
        )

        self.assertDictEqual(calc_tags_eta(["unknow", "tags"]), {})
