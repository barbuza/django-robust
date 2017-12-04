from datetime import timedelta

from django.core.cache import caches
from django.test import SimpleTestCase, override_settings
from django.utils import timezone
from django_redis.cache import RedisCache

from ..models import calc_tags_eta, save_tag_run


@override_settings(ROBUST_RATE_LIMIT={
    'foo': (1, timedelta(seconds=10)),
    'bar': (3, timedelta(minutes=1)),
    'spam': (2, timedelta(minutes=5)),
    'eggs': (1, timedelta(days=1))
})
class TagsTestCase(SimpleTestCase):

    def setUp(self) -> None:
        cache: RedisCache = caches['robust']
        cache.client.get_client().flushall()

    def test_calc(self) -> None:
        start = timezone.now().replace(microsecond=0)
        save_tag_run('foo', start)

        save_tag_run('bar', start)
        save_tag_run('bar', start + timedelta(seconds=5))
        save_tag_run('bar', start + timedelta(seconds=10))

        save_tag_run('spam', start)
        save_tag_run('spam', start + timedelta(minutes=1))
        save_tag_run('spam', start + timedelta(minutes=2))

        self.assertDictEqual(calc_tags_eta(['foo', 'bar', 'spam', 'eggs']), {
            'foo': start + timedelta(seconds=10),
            'bar': start + timedelta(minutes=1),
            'spam': start + timedelta(minutes=6),
        })

        self.assertDictEqual(calc_tags_eta(['unknow', 'tags']), {})
