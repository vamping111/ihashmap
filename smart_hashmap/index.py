import collections
import typing

from smart_hashmap.cache import Cache


class Index:
    """Sub-mapping representation that is stored separately for quick search."""

    cache_name: str
    keys: typing.List[str]

    __INDEXES__ = {}
    """Storage for all existing indexes."""

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, "cache_name"):
            cls.__INDEXES__.setdefault(cls.cache_name, []).append(cls)
        else:
            cls.__INDEXES__.setdefault("__global__", []).append(cls)

        cls.before_create = Cache.PIPELINE_CREATE.add_action("before")(
            cls.before_create
        )
        cls.create = Cache.PIPELINE_CREATE.add_action("after")(cls.create)
        cls.before_update = Cache.PIPELINE_UPDATE.add_action("before")(
            cls.before_update
        )
        cls.update = Cache.PIPELINE_UPDATE.add_action("after")(cls.update)
        cls.before_update = Cache.PIPELINE_DELETE.add_action("before")(
            cls.before_delete
        )
        cls.delete = Cache.PIPELINE_DELETE.add_action("after")(cls.delete)

    @classmethod
    def get_name(cls):
        """Composes index name."""

        keys = "_".join(cls.keys)
        return f"index:{keys}"

    @classmethod
    def get_index(cls, value: typing.Mapping) -> str:
        """Cuts data from value for index storage.

        :param dict value: cached value.
        :return: str: index in string format.
        """

        values = []
        for key in cls.keys:
            values.append(value[key])
        return ":".join(values)

    @classmethod
    def before_create(
        cls,
        ctx: dict,
        cache_cls: typing.Type,
        name: str,
        key: str,
        value: typing.Mapping,
    ):
        """Saves cache name for future use in pipeline.

        :param ctx: Pipeline context.
        :param cache_cls: Cache cls.
        :param name: cache name.
        :param key: stored key.
        :param value: stored value.
        :return: None
        """

        ctx["before_create"] = {"cache_name": name}

    @classmethod
    def create(cls, ctx: dict, result: typing.Mapping):
        """Creates index based on pipeline context and creation result.

        :param dict ctx: Pipeline context.
        :param dict result: value storing result.
        :return:
        """

        cache_name = ctx["before_create"]["cache_name"]
        index_data: list = Cache.get(
            cache_name, cls.get_name(), default=collections.UserList()
        )
        index_data.append(cls.get_index(result))
        Cache.SET_METHOD(cache_name, cls.get_name(), index_data)
        return result

    @classmethod
    def before_delete(
        cls,
        ctx: dict,
        cache_cls: typing.Type,
        name: str,
        key: str,
        value: typing.Mapping,
    ):
        """Saves cache name for future use in pipeline.

        :param ctx: Pipeline context.
        :param cache_cls: Cache cls.
        :param name: cache name.
        :param key: stored key.
        :param value: stored value.
        :return: None
        """

        value = Cache.get(name, key)
        keys = []
        for key in cls.keys:
            keys.append(value.__shadow_copy__[key])
        ctx["before_delete"] = {
            "cache_name": name,
            "keys": ":".join(keys),
        }

    @classmethod
    def delete(cls, ctx: dict, result: typing.Mapping):
        """Deletes index based on pipeline context.

        :param dict ctx: Pipeline context.
        :param dict result: value storing result.
        """

        cache_name = ctx["before_delete"]["cache_name"]
        index_data: list = Cache.get(cache_name, cls.get_name()) or []
        index_data.remove(ctx["before_delete"]["keys"])
        Cache.SET_METHOD(cache_name, cls.get_name(), index_data)
        return result

    @classmethod
    def before_update(
        cls,
        ctx: dict,
        cache_cls: typing.Type,
        name: str,
        key: str,
        value: typing.Mapping,
    ):
        """Saves cache name for future use in pipeline.

        :param ctx: Pipeline context.
        :param cache_cls: Cache cls.
        :param name: cache name.
        :param key: stored key.
        :param value: stored value.
        :return: None
        """

        keys = []
        keys_new = []
        for key in cls.keys:
            keys.append(value.__shadow_copy__[key])
            keys_new.append(value[key])
        ctx["before_update"] = {
            "cache_name": name,
            "keys": ":".join(keys),
            "need_update": keys != keys_new,
            "new_value": value,
        }

    @classmethod
    def update(cls, ctx: dict, result: typing.Mapping):
        """Updates index based on pipeline context.

        :param dict ctx: Pipeline context.
        :param dict result: value storing result.
        """

        if ctx["before_update"]["need_update"]:
            cache_name = ctx["before_update"]["cache_name"]
            index_data: list = Cache.get(cache_name, cls.get_name()) or []
            try:
                index_data.remove(ctx["before_update"]["keys"])
            except ValueError:
                pass
            index_data.append(cls.get_index(ctx["before_update"]["new_value"]))
            Cache.SET_METHOD(cache_name, cls.get_name(), index_data)
        return result

    @classmethod
    def find_index_for_cache(cls, cache_name: str) -> typing.List["Index"]:
        """Finds indexes for specific cache name.

        :param str cache_name: cache name.
        :return: list of matching indexes.
        """

        return cls.__INDEXES__.get(cache_name, []) + cls.__INDEXES__.get(
            "__global__", []
        )

    @classmethod
    def get_values(cls, index_data) -> typing.List[dict]:
        """Formats cache value in dict format.

        :param str index_data: index data.
        :return: list of dicts with index data.
        """

        return [dict(zip(cls.keys, value.split(":"))) for value in index_data]


class PkIndex(Index):
    keys = ["_id"]
