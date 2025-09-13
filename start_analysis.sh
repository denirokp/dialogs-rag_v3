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
echo "📊 Генерируем отчеты..."
python aggregate.py

echo ""
echo "✅ Анализ завершен!"
echo "📁 Результаты сохранены в:"
echo "   - batch_results.csv (таблица результатов)"
echo "   - batch_results.jsonl (детальные данные)"
echo "   - report.md (Markdown отчет)"
echo "   - report.xlsx (Excel отчет)"
echo ""
echo "📊 Для просмотра результатов:"
echo "   cat report.md"
echo "   open report.xlsx"
