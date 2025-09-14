PY=python

# Основные цели DoD
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

# Комплексная система
comprehensive:
	$(PY) comprehensive_dod_pipeline.py --input data/dialogs.xlsx --output artifacts --config final_pipeline_config.json

test:
	$(PY) test_comprehensive_system.py

test-quick:
	$(PY) -c "import asyncio; from test_comprehensive_system import test_file_loading, test_validation; test_file_loading(); test_validation(); print('✅ Быстрые тесты пройдены')"

# Установка зависимостей
install:
	pip install -r requirements.txt
	pip install -r requirements_enhanced.txt
	pip install duckdb pandas numpy umap-learn hdbscan jinja2 redis

# Очистка
clean:
	rm -rf artifacts/*.json artifacts/*.jsonl artifacts/*.npy
	rm -rf logs/*.log
	rm -rf test_results_*

# Полный цикл DoD
dod-full: clean install test comprehensive qa
	@echo "🎯 Полный цикл DoD завершен!"

# Помощь
help:
	@echo "Доступные команды:"
	@echo "  extract     - Извлечение упоминаний (Stage 2)"
	@echo "  dedup       - Дедупликация упоминаний"
	@echo "  cluster     - Кластеризация упоминаний"
	@echo "  summaries   - Построение сводок"
	@echo "  report      - Генерация отчетов"
	@echo "  qa          - Проверки качества DoD"
	@echo "  eval        - Оценка на gold standard"
	@echo "  comprehensive - Запуск комплексной системы"
	@echo "  test        - Полное тестирование системы"
	@echo "  test-quick  - Быстрые тесты"
	@echo "  install     - Установка зависимостей"
	@echo "  clean       - Очистка временных файлов"
	@echo "  dod-full    - Полный цикл DoD"
	@echo "  help        - Эта справка"
