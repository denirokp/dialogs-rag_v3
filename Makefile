include .env

BATCH ?= $(BATCH_ID)
N_DIALOGS ?= $(N_DIALOGS)

PY := python

run-all: ingest extract normalize dedup aggregate quality

ingest:
	$(PY) -m pipeline.ingest_excel --file data/input/dialogs.xlsx --batch $(BATCH)

extract:
	$(PY) -m pipeline.extract_entities --batch $(BATCH)

normalize:
	$(PY) -m pipeline.normalize --batch $(BATCH)

dedup:
	$(PY) -m pipeline.dedup --batch $(BATCH)

# опционально (для "Кластера" в карточках подтем)
cluster:
	$(PY) -m pipeline.cluster_enrich --batch $(BATCH)

aggregate:
	$(PY) -m pipeline.aggregate --batch $(BATCH) --n_dialogs $(N_DIALOGS)

quality:
	$(PY) -m pipeline.quality --batch $(BATCH) --n_dialogs $(N_DIALOGS)

api:
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload