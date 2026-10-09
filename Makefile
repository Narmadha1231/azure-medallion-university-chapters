.PHONY: install run run-api test clean
install:
	python -m pip install -r requirements.txt
run:
	PYTHONPATH=src python -m university_chapters.pipeline --source fixture --inject-dq-fixtures
run-api:
	PYTHONPATH=src python -m university_chapters.pipeline --source api --inject-dq-fixtures
test:
	PYTHONPATH=src pytest -q
clean:
	rm -rf data/bronze/* data/silver/* data/gold/* data/quarantine/* data/run_metrics/* .pytest_cache
