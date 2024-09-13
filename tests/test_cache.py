import collections
import copy
from unittest.mock import MagicMock

import bson
import pytest

from ihashmap.cache import Cache
from ihashmap.index import Index, IndexContainer


@pytest.fixture
def fake_cache():
    return {Index.INDEX_CACHE_NAME: {}}


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


@pytest.fixture(scope="function")
def register_methods(fake_get, fake_set, fake_update, fake_delete):
    Cache.register_get_method(fake_get)
    Cache.register_set_method(fake_set)
    Cache.register_update_method(fake_update)
    Cache.register_delete_method(fake_delete)


@pytest.fixture(autouse=True)
def remove_indexes():
    global_indexes = Index.__INDEXES__["__global__"]
    Index.__INDEXES__.clear()
    Index.__INDEXES__["__global__"] = global_indexes


def test_Cache_simple(fake_cache, register_methods):
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
        "test": {"1234": entity},
        Index.INDEX_CACHE_NAME: {"test:_id": ["1234"], "test:_id_model": ["1234:1"]},
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
    assert cache.search("test", {"model": lambda model: int(model) in [2, 3]}) == [
        entity2,
    ]


def test_IndexContainer_append():

    container = IndexContainer()
    container.append(5)

    assert container == [5]
    container.append(4)
    assert container == [4, 5]

    container = IndexContainer()

    id1 = bson.ObjectId()
    id2 = bson.ObjectId()

    container.append(id2)

    assert container == [id2]

    container.append(id1)
    assert container == [id1, id2]


def test_MultiIndex_search(register_methods, fake_cache, monkeypatch):
    class IndexByModel(Index):
        keys = ["_id", "model"]
        cache_name = "test"

    class IndexByRelease(Index):
        keys = ["_id", "release"]
        cache_name = "test"

    class IndexByABC(Index):
        keys = ["_id", "abc"]
        cache_name = "test"

    cache = Cache()

    entity = collections.UserDict(
        {
            "_id": "1234",
            "model": 1,
            "release": "1.0",
            "abc": 5,
            "other": "value",
            "other2": "value",
        }
    )
    cache.set("test", entity["_id"], entity)

    entity2 = collections.UserDict(
        {
            "_id": "12345",
            "model": 1,
            "release": "1.0",
            "abc": 6,
            "other": "value",
            "other2": "value",
        }
    )
    cache.set("test", entity2["_id"], entity2)

    combine_result = []
    real_combine = copy.deepcopy(Index.combine)

    def fake_combine(cache_name, indexes):
        result = real_combine(cache_name, indexes)
        combine_result.append(result)
        return result

    monkeypatch.setattr(Index, "combine", fake_combine)

    assert cache.search(
        "test",
        {
            "model": 1,
            "release": "1.0",
            "abc": 5,
            "other2": "value",
        },
    ) == [
        entity,
    ]

    assert len(combine_result) == 1

    index_data = list(combine_result[0][0])
    index_keys = combine_result[0][1]

    assert len(index_data) == 2

    assert index_keys == {"_id", "model", "release", "abc"}
    assert index_data == [
        {"_id": "1234", "model": "1", "release": "1.0", "abc": "5"},
        {"_id": "12345", "model": "1", "release": "1.0", "abc": "6"},
    ]

    assert cache.search("test", {"model": 1, "other2": "value",}) == [
        entity,
        entity2,
    ]

    assert len(combine_result) == 2
    index_data = list(combine_result[1][0])
    index_keys = combine_result[1][1]

    assert len(index_data) == 2

    assert index_keys == {"_id", "model"}
    assert index_data == [{"_id": "1234", "model": "1"}, {"_id": "12345", "model": "1"}]
