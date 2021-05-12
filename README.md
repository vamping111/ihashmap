Smart Cache
===========

Wrapper around user defined hash map based storage system.  
Main focus of this project is to implement fast search in dicts  
by using indexes.

Installation
------------

1. Using pip:  
`pip install smart_cache`
   
2. Building from source:  
`make install`
   
How to use
----------

For usage first define `GET_METHOD`, `SET_METHOD`, `UPDATE_METHOD`, `DELETE_METHOD`
with matching signatures using `register_*` methods.
Secondly create required indexes (pk index is required by default).

Now you are all set up to use `Cache.search`

