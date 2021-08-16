all: build install

.PHONY: all test build install lint format sources spec clean-spec rpm srpm

GIT = $(shell which git)
DIST ?= epel-7-x86_64

NAME = ihashmap

VERSION = $(shell rpm -q --qf "%{version}\n" --specfile $(PACKAGE).spec | head -1)
RELEASE = $(shell rpm -q --qf "%{release}\n" --specfile $(PACKAGE).spec | head -1)
PACKAGE = python-$(NAME)
BUILDID = $(shell date --date="$$(git show -s --format=%ci $(HEAD_SHA))" '+%Y%m%d%H%M').git$(HEAD_SHA)
HEAD_SHA = $(shell git rev-parse --short --verify HEAD)

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
	echo rpm -q --specfile $(PACKAGE).spec
	python3 setup.py sdist
	mv dist/$(NAME)-$(VERSION).tar.gz /root/rpmbuild/SOURCES/
	rpmbuild -ba $(PACKAGE).spec

install:
	python3 setup.py install

srpm: sources
	@mkdir -p srpms/
	@rpmbuild -bs --define "_sourcedir $(CURDIR)" \
		--define "_srcrpmdir $(CURDIR)/srpms" $(PACKAGE).spec

rpm: srpm
	@mkdir -p rpms/$(DIST)
	/usr/bin/mock -r $(DIST) --resultdir rpms/$(DIST) \
		--rebuild srpms/$(PACKAGE)-$(VERSION)-$(RELEASE).src.rpm \

spec:
	@git cat-file -p $(HEAD_SHA):$(PACKAGE).spec | sed -e 's,@BUILDID@,$(BUILDID),g' > $(PACKAGE).spec

sources: clean spec
	@git archive --format=tar --prefix=$(PACKAGE)-$(VERSION)/ $(HEAD_SHA) | \
		gzip > $(PACKAGE)-$(VERSION).tar.gz

ifdef GIT
clean-spec:
	@git checkout $(PACKAGE).spec
endif

clean: clean-spec
	@rm -rf build dist srpms rpms $(PACKAGE).egg-info $(PACKAGE)-*.tar.gz *.egg

