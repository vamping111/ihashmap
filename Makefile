all: build install

lint:
	black --check .
	isort --check .

format:
	black .
	isort .

test:
	pytest

build:
	python3 setup.py build

install:
	python3 setup.py install
