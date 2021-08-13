all: build install

.PHONY: all test build install

NAME = ihashmap
VERSION_FILE = .version
VERSION = $(shell cat $(VERSION_FILE))

lint:
	black --check .
	isort --check .
	flake8 .

format:
	black .
	isort .

test:
	pytest

build:
	python3 setup.py sdist
	mv dist/$(NAME)-$(VERSION).tar.gz /root/rpmbuild/SOURCES/
	rpmbuild -ba $(NAME).spec

install:
	python3 setup.py install
