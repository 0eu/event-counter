.PHONY = help test clean install report lint

help: ## This help page.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

test: ## Run tests
	@docker run --rm event-counter -m pytest . -p no:cacheprovider

test_ci: ## Run tests on CI
	python -m pytest .

install: ## Install the package
	docker build -t event-counter .

clean: ## Delete pycache directories
	find . -type d -name __pycache__ -exec rm -r {} \+

report: ## Count the number of events
	docker run --rm -v $(shell pwd)/output:/app/output -v $(shell pwd)/tests/data:/app/input:ro event-counter -m core --events input/$(FILENAME) --report output/report.txt

lint: ## Lint the code
	pre-commit run --all-files
