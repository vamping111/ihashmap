import collections
import functools
import types
import typing

from smart_hashmap.action import Action


class PipelineContext:
    def __init__(self, f, cls_or_self, name, *args, **kwargs):
        self.f = f
        self.cls_or_self = cls_or_self
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.local_data = {}


class Pipeline:
    """Class representation of flow process (Middleware pattern).

    Actions are added to be executed before or after main function execution.
    Actions are executed in their insertion order.
    Each pipeline execution has its context which can be useful for storing
    temporary data between actions.
    """

    def __init__(self, name, parent_pipe=None):
        self.name = name
        self.parent_pipe = parent_pipe
        self._pipe_before = []
        self._pipe_after = []

    @property
    def pipe_before(self):
        pipe = []
        if self.parent_pipe is not None:
            pipe += self.parent_pipe.pipe_before
        pipe += self._pipe_before
        pipe.sort(key=lambda action: action.priority)
        return pipe

    @property
    def pipe_after(self):
        pipe = []
        if self.parent_pipe is not None:
            pipe += self.parent_pipe.pipe_after
        pipe += self._pipe_after
        pipe.sort(key=lambda action: action.priority)
        return pipe

    def before(self, priority=1, cache_name=None):
        def wrapper(f):
            self._pipe_before.append(Action(f, priority, cache_name=cache_name))
            return f

        return wrapper

    def after(self, priority=1, cache_name=None):
        def wrapper(f):
            self._pipe_after.append(Action(f, priority, cache_name=cache_name))
            return f

        return wrapper

    def wrap_before(self, ctx: PipelineContext):
        """Executes all actions in parents _pipe_before and this pipes."""

        for action in self.pipe_before:
            if action.cache_name in [None, ctx.name]:
                action(ctx)

    def wrap_after(self, ctx: PipelineContext):
        """Executes all actions in parents _pipe_after and this pipes."""

        for action in self.pipe_after:
            if action.cache_name in [None, ctx.name]:
                action(ctx)

    def wrap_action(self, ctx: PipelineContext):
        self.wrap_before(ctx)
        ctx.result = ctx.f(ctx.cls_or_self, ctx.name, *ctx.args, **ctx.kwargs)
        self.wrap_after(ctx)
        return ctx.result

    def __call__(self, f: typing.Callable) -> typing.Callable:
        """Wrapper around main function.
        Executes actions before and after main function execution.

        :param f: main function.
        :return: wrapped function.
        """

        @functools.wraps(f)
        def wrap(cls_or_self, name, *args, **kwargs):
            from smart_hashmap.index import Index

            pipeline = self
            if isinstance(cls_or_self, Cache):
                pipeline = getattr(cls_or_self.PIPELINE, self.name)
            elif isinstance(cls_or_self, Index):
                pipeline = getattr(Cache.PIPELINE, self.name)
            ctx = PipelineContext(f, cls_or_self, name, *args, **kwargs)
            return pipeline.wrap_action(ctx)

        return wrap


class PipelineManager:
    """Manager."""

    def __init__(self, parent_manager=None):
        self.pipes = {}
        if parent_manager is not None:
            self.set_parent(parent_manager)

    def __getattr__(self, item):
        if item not in self.pipes:
            self.pipes[item] = Pipeline(item)
        return self.pipes[item]

    def set_parent(self, parent_manager):
        for pipe_name, pipe in parent_manager.pipes.items():
            self.pipes[pipe_name] = Pipeline(pipe.name, parent_pipe=pipe)


class Cache:
    """Wrapper around user-defined caching storage.

    Adds custom logic to plain hash based storage such as indexes
    and quick search based on them.

    For usage first define GET_METHOD, SET_METHOD, UPDATE_METHOD, DELETE_METHOD
    with matching signatures using `register_*` methods.
    Secondly create required indexes (pk index is required by default).
    """

    PIPELINE = PipelineManager()

    PRIMARY_KEY = "_id"
    """Values primary key existing in all values."""

    GET_METHOD = lambda cache, name, key, default=None: None  # noqa: E731
    SET_METHOD = lambda cache, name, key, value: None  # noqa: E731
    UPDATE_METHOD = lambda cache, name, key, value: None  # noqa: E731
    DELETE_METHOD = lambda cache, name, key: None  # noqa: E731
    """METHODS placeholders. You should register yours."""

    @PIPELINE.set
    def set(self, name: str, key: str, value: typing.Mapping):
        """Wrapper for pipeline execution.

        :param str name: cache name.
        :param str key: hash key.
        :param dict value: stored value.
        :return:
        """

        return self.SET_METHOD(name, key, value)

    @PIPELINE.get
    def get(self, name: str, key: str, default: typing.Optional[typing.Any] = None):
        """Wrapper for pipeline execution.

        :param str name: cache name.
        :param str key: hash key.
        :param default: default return value. Must be custom class instance
                        or collections.UserDict/collections.UserList
        :return:
        """

        return self.GET_METHOD(name, key, default)

    @PIPELINE.update
    def update(self, name: str, key: str, value: typing.Mapping):
        """Wrapper for pipeline execution.

        :param str name: cache name.
        :param str key: hash key.
        :param dict value: stored value.
        """

        return self.UPDATE_METHOD(name, key, value)

    @PIPELINE.delete
    def delete(self, name: str, key: str):
        """Wrapper for pipeline execution.

        :param str name: cache name.
        :param str key: hash key.
        """

        return self.DELETE_METHOD(name, key)

    def all(self, name: str):
        """Finds all values in cache.

        :param name:
        :return:
        """

        index_name = f"index:{self.PRIMARY_KEY}"

        index_data = self._get(name, index_name, default=collections.UserList())
        result = []
        for item_key in index_data:
            result.append(self._get(name, item_key))
        return result

    @classmethod
    def register_get_method(cls, method: typing.Callable):
        """Registers get method for global cache usage.

        :param method: function which will be called on .get method execution
        """

        cls.GET_METHOD = method

    @classmethod
    def register_set_method(cls, method: typing.Callable):
        """Registers set method for global cache usage.

        :param method: function which will be called on .set method execution.
        """

        cls.SET_METHOD = method

    @classmethod
    def register_update_method(cls, method: typing.Callable):
        """Registers update method for global cache usage.

        :param method: function which will be called on .update method execution.
        """

        cls.UPDATE_METHOD = method

    @classmethod
    def register_delete_method(cls, method: typing.Callable):
        """Registers update method for global cache usage.

        :param method: function which will be called on .delete method execution.
        :return:
        """
        cls.DELETE_METHOD = method

    @classmethod
    def _match_query(cls, value: dict, query: dict):
        """Matches query to mapping values.

        :param value: value to match against pattern
        :param query: dict se
        :return:
        """

        matched = []
        match = {key: False for key in query}
        for search_key, search_value in query.items():
            if isinstance(search_value, types.FunctionType):
                if search_value(value.get(search_key)):
                    match[search_key] = True
            else:
                if value.get(search_key) == search_value:
                    match[search_key] = True
        if all(match.values()):
            matched.append(value)
        return matched

    def search(
        self,
        name: str,
        search_query: typing.Mapping[
            str, typing.Union[str, int, tuple, list, typing.Callable]
        ],
    ) -> typing.List[typing.Mapping]:
        """Searches cache for required values based on search query.

        :param name: cache name.
        :param dict search_query: search key:value to match.
                                  Values can be any builtin type
                                  or function to which value will be passed as argument.
        :return: list of matching values.
        """

        from smart_hashmap.index import Index

        index_match = []
        indexes = Index.find_index_for_cache(name)
        for index in indexes:
            index_match.append(
                len(set(index.keys).intersection(search_query)) / len(search_query)
            )
        best_choice_index = index_match.index(max(index_match))
        best_index = indexes[best_choice_index]
        index_data = set(best_index.get(name))
        index_data = best_index.get_values(index_data)
        matched = []
        subquery = {
            key: str(value)
            for key, value in search_query.items()
            if key in best_index.keys
        }
        rest_query = {
            key: value
            for key, value in search_query.items()
            if key not in best_index.keys
        }
        for value in index_data:
            matched += self._match_query(value, subquery)
        result = []
        for value in matched:
            entity = self._get(name, value[self.PRIMARY_KEY])
            result += self._match_query(entity, rest_query)
        return result

    @PIPELINE.get
    def _get(self, name: str, key: str, default: typing.Optional[typing.Any] = None):
        """Internal method. PLEASE DONT CHANGE!"""

        return self.GET_METHOD(name, key, default)

    @PIPELINE.set
    def _set(self, name, key, value):
        """Internal method. PLEASE DONT CHANGE!"""

        return self.SET_METHOD(name, key, value)

    @PIPELINE.update
    def _update(self, name, key, value):
        """Internal method. PLEASE DONT CHANGE!"""

        return self.UPDATE_METHOD(name, key, value)

    @PIPELINE.delete
    def _delete(self, name, key):
        """Internal method. PLEASE DONT CHANGE!"""

        return self.DELETE_METHOD(name, key)

    def __init_subclass__(cls, **kwargs):
        cls.PIPELINE = PipelineManager(parent_manager=cls.PIPELINE)


@Cache.PIPELINE.get.after()
def add_shadow_copy(ctx: PipelineContext):
    """Add .__shadow_copy__ attribute for future use in pipelines."""

    if ctx.result is not None:
        ctx.result.__shadow_copy__ = ctx.result
