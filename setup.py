#!/usr/bin/env python

import setuptools


with open(".version") as version_file:
    version = version_file.read()


module_info = {
    "name": "ihashmap",
    "version": version,
    "description": "Wrapper for hash map based storage systems",
    "author": "Yury Sokov aka. Yurzs",
    "author_email": "yurzs+ihashmap@icloud.com",
    "packages": setuptools.find_packages(exclude=("tests", "tests.*", "*.tests"),
                                         include=[".version", "ihashmap"]),
    "license": "MIT",
    "keywords": ["cache", "indexes"],
    "url": "https://github.com/yurzs/smart_hashmap",
    "classifiers": [
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
}

with open("README.rst") as long_description_file:
    module_info["long_description"] = long_description_file.read()

setuptools.setup(**module_info)
