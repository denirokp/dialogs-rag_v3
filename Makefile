PY=python

extract:
	@echo ">> run your Stage 2 extractor to produce mentions.jsonl (client-only + evidence)"

dedup:
	$(PY) scripts/dedup.py --in mentions.jsonl --out mentions_dedup.jsonl

cluster:
	@echo ">> run scripts/clusterize.py per subtheme with your embeddings"

summaries:
	duckdb -c ".read sql/build_summaries.sql"

report:
	@echo ">> render jinja templates using your data loader"

qa:
	$(PY) quality/run_checks.py

eval:
	$(PY) scripts/eval_extraction.py --gold goldset/gold.jsonl --pred mentions_dedup.jsonl
