import collections
import typing

from smart_hashmap.cache import Cache, PipelineContext


class Index:
    """Sub-mapping representation that is stored separately for quick search."""

    cache_name: str
    keys: typing.List[str]

    __INDEXES__ = {}
    """Storage for all existing indexes."""

    HOOKS = [
        ("before_create", Cache.PIPELINE_CREATE),
        ("after_create", Cache.PIPELINE_CREATE),
        ("before_get", Cache.PIPELINE_GET),
        ("after_get", Cache.PIPELINE_GET),
        ("before_update", Cache.PIPELINE_UPDATE),
        ("after_update", Cache.PIPELINE_UPDATE),
        ("before_delete", Cache.PIPELINE_DELETE),
        ("after_delete", Cache.PIPELINE_DELETE),
    ]

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, "cache_name"):
            cls.__INDEXES__.setdefault(cls.cache_name, []).append(cls)
        else:
            cls.__INDEXES__.setdefault("__global__", []).append(cls)

        for hook, pipe in cls.HOOKS:
            if hasattr(cls, hook):
                hook_action = getattr(cls, hook)
                setattr(cls, hook, pipe.add_action(hook.split("_")[0])(hook_action))

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
    def after_create(cls, ctx: PipelineContext):
        """Creates index based on pipeline context and creation result.

        :param ctx: Pipeline context.
        :return:
        """

        index_data: list = Cache.get(
            ctx.name, cls.get_name(), default=collections.UserList()
        )
        index_data.append(cls.get_index(ctx.result))
        Cache.SET_METHOD(ctx.name, cls.get_name(), index_data)

    @classmethod
    def before_delete(cls, ctx: PipelineContext):
        """Saves cache name for future use in pipeline.

        :param ctx: Pipeline context.
        """

        value = Cache.get(ctx.name, ctx.args[0])
        keys = []
        for key in cls.keys:
            keys.append(value.__shadow_copy__[key])
        ctx.local_data["before_delete"] = {"keys": ":".join(keys)}

    @classmethod
    def after_delete(cls, ctx: PipelineContext):
        """Deletes index based on pipeline context.

        :param dict ctx: Pipeline context.
        """

        index_data: list = Cache.get(ctx.name, cls.get_name()) or []
        index_data.remove(ctx.local_data["before_delete"]["keys"])
        Cache.SET_METHOD(ctx.name, cls.get_name(), index_data)

    @classmethod
    def after_update(cls, ctx: PipelineContext):
        """Updates index based on pipeline context.

        :param dict ctx: Pipeline context.
        """

        if cls.get_index(ctx.args[0].__shadow_copy__) != cls.get_index(ctx.result):
            index_data: list = Cache.get(ctx.name, cls.get_name()) or []
            try:
                index_data.remove(cls.get_index(ctx.args[0].__shadow_copy__))
            except ValueError:
                pass
            index_data.append(cls.get_index(ctx.result))
            Cache.SET_METHOD(ctx.name, cls.get_name(), index_data)

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
