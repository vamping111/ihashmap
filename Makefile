all: build install

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
	python3 setup.py build

install:
	python3 setup.py install
