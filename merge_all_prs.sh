#!/bin/bash

echo "Начинаем процесс принятия всех пул-реквестов..."

# Переходим в директорию проекта
cd /Users/Work/Documents/dialogs-rag-v2

# Обновляем информацию о репозитории
echo "Обновляем информацию о репозитории..."
git fetch --all

# Переключаемся на main ветку
echo "Переключаемся на main ветку..."
git checkout main

# Обновляем main ветку
echo "Обновляем main ветку..."
git pull origin main

# Список всех веток с пул-реквестами
branches=(
    "origin/codex/add-cosine-similarity-check-in-dedup_mentions"
    "origin/codex/add-input_xlsx-argument-to-analyze_dialogs_advanced.py"
    "origin/codex/add-progress-bar-and-logging-to-run"
    "origin/codex/add-reload-button-with-cache-clear"
    "origin/codex/add-tiktoken-dependency-and-tokenization"
    "origin/codex/fix-build_map-to-improve-file-handling"
    "origin/codex/implement-global-file-cache-with-timestamp"
    "origin/codex/load-taxonomy.yaml-in-llm-constructor"
    "origin/codex/update-groupby-parameters-in-simple_dashboard.py"
    "origin/codex/wrap-openai-requests-in-try/except"
)

# Принимаем каждый пул-реквест
for branch in "${branches[@]}"; do
    echo "Принимаем пул-реквест: $branch"
    
    # Пытаемся слить ветку
    if git merge "$branch" --no-edit; then
        echo "✅ Успешно принят: $branch"
    else
        echo "❌ Конфликт в: $branch"
        echo "Требуется ручное разрешение конфликтов"
        echo "Выполните: git status для просмотра конфликтующих файлов"
        exit 1
    fi
done

echo "🎉 Все пул-реквесты успешно приняты!"

# Отправляем изменения
echo "Отправляем изменения в удаленный репозиторий..."
git push origin main

echo "✅ Готово! Все изменения отправлены в GitHub."
