install:
	poetry install --with dev

lint:
	poetry run isort ./opensearch_reindexer ./tests/e2e
	poetry run black ./opensearch_reindexer ./tests/e2e

up:
	docker-compose up -d

down:
	docker-compose down

test:
	poetry run pytest --cov ./opensearch_reindexer --cov-report=term-missing --cov-report=xml -s

publish:
	poetry build
	poetry publish
