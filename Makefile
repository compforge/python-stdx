.PHONY: fix lint test

fix:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy src

test:
	uv run pytest

