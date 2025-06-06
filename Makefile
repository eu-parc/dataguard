SET ?=

test:
	bash scripts/test.sh

lint:
	bash scripts/lint.sh

format:
	bash scripts/format.sh

test-set:
	bash scripts/test_subset.sh $(SET)