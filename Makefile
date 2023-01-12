install:
	poetry install --with dev

lint:
	isort ./opensearch_reindexer ./tests/e2e
	black ./opensearch_reindexer ./tests/e2e

up:
	docker-compose up -d

down:
	docker-compose down

test:
	pytest --cov ./opensearch_reindexer --cov-report=term-missing --cov-report=xml -s

publish:
	poetry build
	poetry publish
