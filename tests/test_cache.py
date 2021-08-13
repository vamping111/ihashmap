import collections
from unittest.mock import MagicMock

import pytest

from ihashmap.cache import Cache
from ihashmap.index import Index


@pytest.fixture
def fake_cache():
    return {}


@pytest.fixture
def fake_get(fake_cache):
    def _get(self, name, key, default=None):
        return fake_cache[name].get(key, default)

    return _get


@pytest.fixture
def fake_set(fake_cache):
    def _set(self, name, key, value):
        fake_cache.setdefault(name, {})[key] = value
        return value

    return _set


@pytest.fixture
def fake_update(fake_cache):
    return fake_cache.update


@pytest.fixture
def fake_delete(fake_cache):
    def _delete(self, name, key):
        del fake_cache[name][key]

    return _delete


def test_Cache_simple(fake_cache, fake_get, fake_set, fake_update, fake_delete):
    Cache.register_get_method(fake_get)
    Cache.register_set_method(fake_set)
    Cache.register_update_method(fake_update)
    Cache.register_delete_method(fake_delete)

    class IndexByModel(Index):
        keys = ["_id", "model"]
        cache_name = "test"

    cache = Cache()

    entity = collections.UserDict({"_id": "1234", "model": 1, "release": "1.0"})
    cache.set("test", "1234", entity)
    assert cache.get("test", "1234") == entity
    assert cache.all("test") == [
        entity,
    ]

    assert fake_cache == {
        "test": {"1234": entity, "index:_id": ["1234"], "index:_id_model": ["1234:1"]}
    }

    assert cache.search("test", {"model": 1}) == [
        entity,
    ]

    class Cache2(Cache):
        pass

    mocked_func = MagicMock()
    Cache2.PIPELINE.get.before()(mocked_func)

    cache2 = Cache2()
    cache = Cache()

    cache2.get("test", "test")

    assert mocked_func.called
    assert mocked_func.call_count == 1

    cache.get("test", "1234")
    assert mocked_func.call_count == 1

    entity2 = collections.UserDict({"_id": "3456", "model": 2})
    cache.set("test", "3456", entity2)
    assert cache.search("test", {"model": lambda model: model in ["2", "3"]}) == [
        entity2,
    ]
