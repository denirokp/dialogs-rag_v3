# Dialogs RAG System v2.0 Makefile

.PHONY: help install dev-install test lint format clean run setup

# Default target
help:
	@echo "Dialogs RAG System v2.0 - Available commands:"
	@echo ""
	@echo "  setup       - Initial setup (install dependencies, create directories)"
	@echo "  install     - Install production dependencies"
	@echo "  dev-install - Install development dependencies"
	@echo "  test        - Run tests"
	@echo "  lint        - Run linting checks"
	@echo "  format      - Format code with black"
	@echo "  clean       - Clean temporary files and caches"
	@echo "  run         - Run the main pipeline with example data"
	@echo "  help        - Show this help message"

# Initial setup
setup: install dev-install
	@echo "Creating necessary directories..."
	@mkdir -p data/input data/output logs models tests
	@touch data/input/.gitkeep data/output/.gitkeep logs/.gitkeep models/.gitkeep
	@echo "Setup complete!"

# Install production dependencies
install:
	@echo "Installing production dependencies..."
	pip install -r requirements.txt

# Install development dependencies
dev-install: install
	@echo "Installing development dependencies..."
	pip install pytest pytest-cov black flake8 mypy

# Run tests
test:
	@echo "Running tests..."
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Run linting
lint:
	@echo "Running linting checks..."
	flake8 src/ --max-line-length=100 --ignore=E203,W503
	mypy src/ --ignore-missing-imports

# Format code
format:
	@echo "Formatting code with black..."
	black src/ --line-length=100
	black main.py --line-length=100

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -delete
	find . -type d -name ".mypy_cache" -delete
	find . -type d -name "htmlcov" -delete
	rm -rf dist/ build/ *.egg-info/

# Run the main pipeline
run:
	@echo "Running main pipeline..."
	python main.py --input data/input/dialogs.xlsx --verbose

# Run with dry-run
dry-run:
	@echo "Running dry-run..."
	python main.py --input data/input/dialogs.xlsx --dry-run --verbose

# Create example data
example-data:
	@echo "Creating example data..."
	@mkdir -p data/input
	@echo "Creating sample dialogs.xlsx..."
	python -c "import pandas as pd; import numpy as np; data = {'dialog_id': range(1, 101), 'text': [f'Диалог {i}: Проблема с доставкой заказа {i}. Нужно улучшить логистику.' for i in range(1, 101)], 'timestamp': pd.date_range('2024-01-01', periods=100, freq='1H'), 'user_id': np.random.randint(1, 20, 100)}; df = pd.DataFrame(data); df.to_excel('data/input/dialogs.xlsx', index=False); print('Example data created: data/input/dialogs.xlsx')"

# Full pipeline test
test-pipeline: example-data
	@echo "Testing full pipeline..."
	python main.py --input data/input/dialogs.xlsx --verbose

# Development mode
dev: clean setup
	@echo "Development environment ready!"
	@echo "Run 'make test-pipeline' to test the pipeline"

# Production build
build: clean format lint test
	@echo "Production build complete!"

# Docker build (if Dockerfile exists)
docker-build:
	@echo "Building Docker image..."
	docker build -t dialogs-rag-v2 .

# Docker run
docker-run:
	@echo "Running Docker container..."
	docker run -v $(PWD)/data:/app/data dialogs-rag-v2
