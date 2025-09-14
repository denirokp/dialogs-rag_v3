#!/usr/bin/env python3
"""
Быстрый тест Dialogs RAG на реальных данных
"""
import os
import subprocess
import sys
import time

def run_command(cmd, description):
    """Выполнить команду и показать результат"""
    print(f"\n🔄 {description}...")
    print(f"Команда: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            print(f"✅ {description} - успешно")
            if result.stdout.strip():
                print(f"Вывод: {result.stdout.strip()}")
        else:
            print(f"❌ {description} - ошибка")
            print(f"Ошибка: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏰ {description} - таймаут (5 минут)")
        return False
    except Exception as e:
        print(f"💥 {description} - исключение: {e}")
        return False
    
    return True

def main():
    print("🚀 Быстрый тест Dialogs RAG")
    print("=" * 50)
    
    # Проверяем наличие файла данных
    data_file = "data/input/dialogs.xlsx"
    if not os.path.exists(data_file):
        print(f"❌ Файл {data_file} не найден!")
        print("Создайте файл с колонками: dialog_id, raw_text")
        print("Пример raw_text: 'client: хочу включить доставку\\noperator: проверьте настройки'")
        return False
    
    # Устанавливаем переменные окружения
    os.environ["BATCH_ID"] = "test-2025-01-14"
    os.environ["N_DIALOGS"] = "1000"  # Меньше для быстрого теста
    os.environ["EXTRACT_MODE"] = "RULES"
    os.environ["DUCKDB_PATH"] = "data/test_rag.duckdb"
    os.environ["REQUIRE_QUALITY_PASS"] = "true"
    
    # Последовательность команд
    commands = [
        ("pip install -r requirements.txt", "Установка зависимостей"),
        ("make ingest BATCH=test-2025-01-14", "Загрузка данных"),
        ("make extract BATCH=test-2025-01-14", "Извлечение сущностей"),
        ("make normalize BATCH=test-2025-01-14", "Нормализация"),
        ("make dedup BATCH=test-2025-01-14", "Дедупликация"),
        ("make aggregate BATCH=test-2025-01-14 N_DIALOGS=1000", "Агрегация"),
        ("make quality BATCH=test-2025-01-14 N_DIALOGS=1000", "Проверка качества"),
    ]
    
    # Выполняем команды
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            print(f"\n❌ Тест остановлен на этапе: {desc}")
            return False
    
    # Запускаем API в фоне
    print(f"\n🌐 Запуск API...")
    api_process = subprocess.Popen([
        sys.executable, "-m", "uvicorn", "api.main:app", 
        "--host", "0.0.0.0", "--port", "8000"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Ждем запуска API
    time.sleep(3)
    
    # Тестируем API
    api_commands = [
        ("curl -s http://localhost:8000/api/quality", "Проверка качества"),
        ("curl -s http://localhost:8000/api/summary_themes", "Сводка по темам"),
        ("curl -s 'http://localhost:8000/api/index_quotes?page=1&page_size=5'", "Индекс цитат"),
    ]
    
    print(f"\n🧪 Тестирование API...")
    for cmd, desc in api_commands:
        if not run_command(cmd, desc):
            print(f"⚠️  API тест не прошел: {desc}")
    
    # Останавливаем API
    api_process.terminate()
    api_process.wait()
    
    print(f"\n🎉 Тест завершен!")
    print(f"📊 Результаты сохранены в data/warehouse/")
    print(f"🔍 Для детального анализа запустите: make api")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

