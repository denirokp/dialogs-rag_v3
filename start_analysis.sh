#!/bin/bash

echo "🚀 Запуск анализа диалогов RAG"
echo "================================"

# Проверяем наличие Python
if ! command -v python &> /dev/null; then
    echo "❌ Python не найден. Установите Python 3.8+"
    exit 1
fi

# Проверяем наличие файла данных
if [ ! -f "data/dialogs.xlsx" ]; then
    echo "❌ Файл data/dialogs.xlsx не найден"
    echo "   Поместите ваш Excel файл в папку data/"
    exit 1
fi

# Устанавливаем зависимости если нужно
echo "📦 Проверяем зависимости..."
pip install -r requirements.txt > /dev/null 2>&1

# Запускаем анализ
echo "🔍 Запускаем анализ..."
python run_analysis.py

echo ""
echo "✅ Анализ завершен!"
echo "📁 Результаты сохранены в:"
echo "   - batch_results.csv"
echo "   - batch_results.jsonl"
echo ""
echo "📊 Для просмотра результатов:"
echo "   cat batch_results.csv"
echo "   cat batch_results.jsonl"
