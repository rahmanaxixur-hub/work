list-gcs:
	poetry run python src/list_gcs_files.py
# Makefile for common project tasks

.PHONY: install test run lint

install:
	poetry install

test:
	poetry run pytest

fake-users-gen-and-upload-to-gcs:
	poetry run python src/generate_fake_users.py

lint:
	poetry run flake8 src/
