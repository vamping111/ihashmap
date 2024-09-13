import bisect
import collections
import typing

from ihashmap.cache import Cache, PipelineContext


class IndexContainer(collections.UserList):
    def append(self, item) -> None:
        bisect.insort(self.data, item)

    def insert(self, i: int, item) -> None:
        bisect.insort(self.data, item)


class Index:
    """Sub-mapping representation that is stored separately for quick search."""

    INDEX_CACHE_NAME: str = "indexes"
    cache_name: str = None
    keys: typing.List[str]

    __INDEXES__ = {}
    """Storage for all existing indexes."""

    HOOKS = [
        ("before_create", Cache.PIPELINE.set.before),
        ("after_create", Cache.PIPELINE.set.after),
        ("before_get", Cache.PIPELINE.get.before),
        ("after_get", Cache.PIPELINE.get.after),
        ("before_update", Cache.PIPELINE.update.before),
        ("after_update", Cache.PIPELINE.update.after),
        ("before_delete", Cache.PIPELINE.delete.before),
        ("after_delete", Cache.PIPELINE.delete.after),
    ]

    def __init_subclass__(cls, **kwargs):
        if cls.cache_name is not None:
            cls.__INDEXES__.setdefault(cls.cache_name, []).append(cls)
        else:
            cls.__INDEXES__.setdefault("__global__", []).append(cls)

        for hook, pipe_wrapper in cls.HOOKS:
            if hasattr(cls, hook):
                hook_action = getattr(cls, hook)
                setattr(cls, hook, pipe_wrapper(cache_name=cls.cache_name)(hook_action))

        # TODO: rebuild index?

    @classmethod
    def get_name(cls, cache_name):
        """Composes index name."""

        keys = "_".join(cls.keys)
        return f"{cache_name}:{keys}"

    @classmethod
    def get_index(cls, value: typing.Mapping) -> str:
        """Cuts data from value for index storage.

        :param dict value: cached value.
        :return: str: index in string format.
        """

        values = []
        for key in cls.keys:
            values.append(value[key])
        return ":".join(str(value) for value in values)

    @classmethod
    def before_create(cls, ctx: PipelineContext):
        """Stores original value for after_create usage."""

        key, value = ctx.args
        ctx.local_data["original_value"] = value

    @classmethod
    def after_create(cls, ctx: PipelineContext):
        """Creates index based on pipeline context and creation result.

        :param ctx: PipelineManager context.
        :return:
        """

        value = ctx.local_data["original_value"]
        index_data = set(cls.get(ctx.name))
        index_data.add(cls.get_index(value))
        index_data = IndexContainer(index_data)
        cls.set(ctx.name, index_data)

    @classmethod
    def before_delete(cls, ctx: PipelineContext):
        """Saves cache name for future use in pipeline.

        :param ctx: PipelineManager context.
        """

        (key,) = ctx.args
        cache = ctx.cls_or_self
        value = cache._get(ctx.name, key)
        keys = []
        for index_key in cls.keys:
            keys.append(str(value.__shadow_copy__[index_key]))
        ctx.local_data.setdefault("before_delete", {})[cls.__name__] = {
            "keys": ":".join(keys)
        }

    @classmethod
    def after_delete(cls, ctx: PipelineContext):
        """Deletes index based on pipeline context.

        :param dict ctx: PipelineManager context.
        """

        index_data = set(cls.get(ctx.name))
        index_data.remove(ctx.local_data["before_delete"][cls.__name__]["keys"])
        index_data = IndexContainer(index_data)
        cls.set(ctx.name, index_data)

    @classmethod
    def before_update(cls, ctx: PipelineContext):
        """Creates value copy for after_update usage."""

        key, value = ctx.args
        ctx.local_data["original_value"] = value

    @classmethod
    def after_update(cls, ctx: PipelineContext):
        """Updates index based on pipeline context.

        :param dict ctx: PipelineManager context.
        """

        value = ctx.local_data["original_value"]
        if cls.get_index(value.__shadow_copy__) != cls.get_index(ctx.result):
            index_data = set(cls.get(ctx.name))
            try:
                index_data.remove(cls.get_index(value.__shadow_copy__))
            except ValueError:
                pass
            index_data.add(cls.get_index(ctx.result))
            index_data = IndexContainer(index_data)
            cls.set(ctx.name, index_data)

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
    def get_values(
        cls, index_data: typing.Union[typing.List, typing.Tuple, typing.Set]
    ) -> typing.List[dict]:
        """Formats cache value in dict format.

        :param str index_data: index data.
        :return: list of dicts with index data.
        """

        return [dict(zip(cls.keys, value.split(":"))) for value in index_data]

    @classmethod
    @Cache.PIPELINE.index_get
    def get(cls, cache_name):
        return Cache.GET_METHOD(
            Cache,
            cls.INDEX_CACHE_NAME,
            cls.get_name(cache_name),
            default=IndexContainer(),
        )

    @classmethod
    @Cache.PIPELINE.index_set
    def set(cls, cache_name, value: IndexContainer):
        return Cache.SET_METHOD(
            Cache, cls.INDEX_CACHE_NAME, cls.get_name(cache_name), value
        )

    @classmethod
    def set_index_cache_name(cls, index_cache_name: str):
        cls.INDEX_CACHE_NAME = index_cache_name

    @classmethod
    def combine(
        cls, cache_name: str, indexes: typing.List["Index"]
    ) -> (typing.List[typing.Dict[str, typing.Any]], set):
        """Combines indexes into one."""

        combined_index_data = {}
        combined_index_keys = set()

        for index in indexes:
            for index_container in index.get_values(index.get(cache_name)):
                index_data = {key: value for key, value in index_container.items()}

                combined_index_data.setdefault(index_data["_id"], {}).update(index_data)

            combined_index_keys.update(index.keys)

        return (
            sorted(combined_index_data.values(), key=lambda d: d["_id"]),
            combined_index_keys,
        )


class PkIndex(Index):
    keys = ["_id"]
