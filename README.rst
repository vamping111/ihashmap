.. role:: bash(code)
   :language: bash
 
.. role:: python3(code)
   :language: python3

Smart Hashmap
=============

.. image:: https://github.com/Yurzs/smart_hashmap/actions/workflows/python-on-pull-request.yml/badge.svg
    :alt: Lint and Test
    :target: https://github.com/Yurzs/smart_hashmap/actions/workflows/python-on-pull-request.yml

.. image:: https://raw.github.com/yurzs/smart_hashmap/master/assets/hashmap-logo.svg
    :alt: Smart Hashmap logo

Wrapper for key-value based storage systems. Provides convenient way to organize data for quick searching.

Installation
------------

1. Using pip:  
:bash:`pip install smart_hashmap`
   
2. Building from source:  
:bash:`make install`
   
How to use
----------

Firstly you need to register methods:

.. code-block:: python3

    from smart_hashmap.cache import Cache

    Cache.register_get_method(YOUR_GET_METHOD)
    Cache.register_set_method(YOUR_SET_METHOD)
    Cache.register_update_method(YOUR_UPDATE_METHOD)
    Cache.register_delete_method(YOUR_DELETE_METHOD)

NOTE: Methods signature MUST match their placeholders signature

.. code-block:: python3

    GET_METHOD = lambda cache, name, key, default=None: None  # noqa: E731
    SET_METHOD = lambda cache, name, key, value: None  # noqa: E731
    UPDATE_METHOD = lambda cache, name, key, value: None  # noqa: E731
    DELETE_METHOD = lambda cache, name, key: None  # noqa: E731
    """METHODS placeholders. You should register yours."""


Now you are all set up to use :python3:`Cache.search`

How it works
------------

In default setup :python3:`Cache` creates and maintains indexes based on :python3:`Cache.primary_key`.  

So every object save in cache MUST have such key. (By default its :python3:`_id`)

On every called action for example :python3:`Cache.update` 
Cache looks in pipeline :python3:`Cache.PIPELINE.update` for middlewares to run before and after main function execution.
For example in current situation after `.update` function execution indexing middleware will
check if documents fields matching its keys were changed.  
If so it will get index data, look for old values in :python3:`value.__shadow_copy__` 
remove such index data and create new record with updated values.

Adding middlewares
------------------

Adding new action is easy:

.. code-block:: python3

    from smart_hashmap.cache import Cache, PipelineContext

    @Cache.PIPELINE.set.before()
    def add_my_field(ctx: PipelineContext):

        key, value = ctx.args
        value["my_field"] = 1


Now every cache value saved with :python3:`Cache.set` will be added :python3:`'my_field'` 
before main function execution.

Custom Indexes
--------------

To create custom index you need to simply create new subclass of Index.

.. code-block:: python3

    from smart_hashmap.index import Index

    class IndexByModel(Index):
        keys = ["_id", "model"]


NOTE: After that all values MUST have fields :python3:`_id` AND :python3:`model`  

NOTE: Primary key MUST ALWAYS be in :python3:`keys`

Searching 
---------

After all required indexes created - searching will be as quick as possible.

.. code-block:: python3

    from smart_hashmap.cache import Cache
    from smart_hashmap.index import Index

    class IndexByModel(Index):
        keys = ["_id", "model"]

    cache = Cache()
    cache.search("my_cache", {"model": "1.0"})

When :python3:`.search` is called it will firstly check for indexes containing search fields.  
After finding best index, it will get index data and find matching primary keys.
Now searching is as easy as getting values by their key.
