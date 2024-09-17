import json
import threading
from functools import partial
from types import FunctionType
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, TypeVar, Union

from ihashmap.cache import Cache, PipelineContext
from ihashmap.helpers import match_query

T = TypeVar("T")


class Index:
    """Sub-mapping representation that is stored separately for quick search."""

    INDEX_CACHE_PREFIX: str = "_index_"
    REVERSE_CACHE_INDEX_PREFIX: str = "_reverse_index_"

    KEY_SEPARATOR = "\u00A0"
    PK_KEY_PLACEHOLDER = f"{KEY_SEPARATOR}pk{KEY_SEPARATOR}"

    cache_name: str = None
    keys: List[str]
    unique: bool = False

    LOCK = threading.Lock()

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

    def __init_subclass__(cls):
        """Registers index in global storage."""

        if cls.cache_name is not None:
            cls.__INDEXES__.setdefault(cls.cache_name, []).append(cls)
        else:
            cls.__INDEXES__.setdefault("__global__", []).append(cls)

        cls.keys = list(sorted(set(cls.keys)))

        for hook, pipe_wrapper in cls.HOOKS:
            if hasattr(cls, hook):
                hook_action = getattr(cls, hook)
                setattr(cls, hook, pipe_wrapper(cache_name=cls.cache_name)(hook_action))

    @classmethod
    def get_keys(cls) -> List[str]:
        """Returns index keys with rendered primary key."""

        result = []
        for key in cls.keys:
            result.append(key if key != cls.PK_KEY_PLACEHOLDER else cls.cache().PRIMARY_KEY)

        return result

    @classmethod
    def cache(cls) -> "Cache":
        """Returns cache instance."""

        return Cache.instance()

    @classmethod
    def get_name(cls, cache_name: str, reverse: bool = False):
        """Composes index name."""

        keys = "_".join(cls.get_keys())
        prefix = cls.REVERSE_CACHE_INDEX_PREFIX if reverse else cls.INDEX_CACHE_PREFIX

        return f"{prefix}:{cache_name}:{keys}"

    @classmethod
    def get_key(cls, value: Mapping[str, Any]) -> str:
        """Returns index key for value.

        :param dict value: cached value.
        :return: str: index in string format.
        """

        return json.dumps(cls.cut_data(value), sort_keys=True)

    @classmethod
    def cut_data(cls, value: Mapping, exclude_none: bool = False) -> Dict[str, Any]:
        """Cuts data from value for index storage.

        :param dict value: cached value.
        :param bool exclude_none: exclude None values.
        :return: str: index in string format.
        """

        result = {}

        for key in cls.get_keys():
            result[key] = value.get(key)

        return (
            {k: v for k, v in result.items() if v is not None}
            if exclude_none
            else result
        )

    @classmethod
    def before_create(cls, ctx: PipelineContext):
        """Stores original value for after_create usage."""

        key, value = ctx.args
        ctx.local_data["original_value"] = value

        if cls.unique:
            index_key = cls.get_key(value)

            if cls.get(ctx.name, index_key):
                raise ValueError(f"Unique index violation {msgpack.loads(index_key, raw=False)}")

    @classmethod
    def after_create(cls, ctx: PipelineContext):
        """Creates index based on pipeline context and creation result.

        :param ctx: PipelineManager context.
        :return:
        """

        value: Mapping = ctx.local_data["original_value"]

        with cls.LOCK:
            pk = value[cls.cache().PRIMARY_KEY]
            key = cls.get_key(value)

            index_value: Set[str] = set(cls.get(ctx.name, key, default=[]))
            index_value.add(pk)

            cls.set(ctx.name, cls.get_key(value), list(index_value))
            cls.set(ctx.name, pk, key, reverse=True)

    @classmethod
    def before_delete(cls, ctx: PipelineContext):
        """Saves cache name for future use in pipeline.

        :param ctx: PipelineManager context.
        """

        ctx.local_data["pk"] = cls.cache().protocol.get(
            ctx.name, ctx.args[0][cls.cache().PRIMARY_KEY]
        )

    @classmethod
    def after_delete(cls, ctx: PipelineContext):
        """Deletes index based on pipeline context.

        :param dict ctx: PipelineManager context.
        """

        with cls.LOCK:
            key = cls.get(ctx.name, ctx.local_data["pk"], reverse=True)

            if key is not None:
                cls.delete(ctx.name, key)
                cls.delete(ctx.name, ctx.local_data["pk"], reverse=True)

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

        with cls.LOCK:
            key = cls.get_key(value)
            pk = value[cls.cache().PRIMARY_KEY]

            index_key = cls.get(ctx.name, pk, reverse=True)
            if index_key is not None:
                cls.delete(ctx.name, index_key)
                cls.delete(ctx.name, pk, reverse=True)

            cls.set(ctx.name, key, pk)
            cls.set(ctx.name, pk, key, reverse=True)

    @classmethod
    def find_index_for_cache(cls, cache_name: str) -> List["Index"]:
        """Finds indexes for specific cache name.

        :param str cache_name: cache name.
        :return: list of matching indexes.
        """

        return cls.__INDEXES__.get(cache_name, []) + cls.__INDEXES__.get(
            "__global__", []
        )

    @classmethod
    @Cache.PIPELINE.index_get
    def get(
        cls,
        cache_name: str,
        key: str,
        reverse: bool = False,
        default: Optional[T] = None,
    ) -> Union[List[str], str, T]:
        return cls.cache().protocol.get(
            cls.get_name(cache_name, reverse=reverse),
            key,
            default=default,
        )

    @classmethod
    def keys_(cls, cache_name: str, reverse: bool = False):
        return cls.cache().protocol.keys(cls.get_name(cache_name, reverse=reverse))

    @classmethod
    @Cache.PIPELINE.index_set
    def set(
        cls, cache_name, key: str, value: Union[List[str], str], reverse: bool = False
    ) -> None:
        return cls.cache().protocol.set(
            cls.get_name(cache_name, reverse=reverse), key, value
        )

    @classmethod
    @Cache.PIPELINE.index_delete
    def delete(cls, cache_name: str, key: str, reverse: bool = False) -> None:
        return cls.cache().protocol.delete(
            cls.get_name(cache_name, reverse=reverse), key
        )

    @classmethod
    def combine(
        cls,
        cache_name: str,
        indexes: List["Index"],
        query: Mapping[str, Any],
    ) -> Tuple[List[str], Set[str]]:
        """Combines indexes into one."""

        combined_index_keys = set()
        matches: List[Mapping[str, Any]] = []

        cache_pk = cls.cache().PRIMARY_KEY

        for index in indexes:
            subquery = index.cut_data(query, exclude_none=True)

            func_search = any(
                isinstance(value, FunctionType) for value in subquery.values()
            )

            if func_search or index.cut_data(query) != subquery:
                index_data = [json.loads(d) for d in index.keys_(cache_name)]

                for key, value in subquery.items():
                    filter_func = (
                        partial(lambda v, f, k: f(v.get(k)), f=value, k=key)
                        if isinstance(value, FunctionType)
                        else partial(lambda v, t, k: v.get(k) == t, t=value, k=key)
                    )
                    index_data = filter(filter_func, index_data)

                matches.extend(
                    {cache_pk: pk, **index.cut_data(i)}
                    for i in index_data
                    for pk in index.get(cache_name, index.get_key(i), default=[])
                )

            else:
                search_key = index.get_key(subquery)
                matches.extend(
                    {cache_pk: pk, **index.cut_data(subquery)}
                    for pk in index.get(cache_name, search_key, default=[])
                )

        combined_index = {}
        for match in matches:
            combined_index.setdefault(match[cache_pk], {}).update(match)

        result = []

        indexed_query = {
            key: value for key, value in query.items() if key in combined_index_keys
        }

        for doc in combined_index.values():
            if match_query(doc, indexed_query):
                result.append(doc)

        return (
            sorted(result, key=lambda v: v[cache_pk]),
            combined_index_keys,
        )


class PkIndex(Index):
    keys = [Index.PK_KEY_PLACEHOLDER]
    unique = True
