#!/usr/bin/env python3
"""
Скрипт для сохранения текущей системы анализа диалогов
"""

import os
import shutil
import json
import subprocess
from pathlib import Path
from datetime import datetime
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_backup():
    """Создание резервной копии системы"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = Path(f"backups/system_backup_{timestamp}")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Создание резервной копии в {backup_dir}")
    
    # Копируем основные файлы
    files_to_backup = [
        "simple_api.py",
        "simple_dashboard.py", 
        "analysis_summary.md",
        "README_ANALYSIS.md",
        "requirements_unified.txt",
        "Makefile_unified",
        "taxonomy.yaml"
    ]
    
    for file in files_to_backup:
        if Path(file).exists():
            shutil.copy2(file, backup_dir / file)
            logger.info(f"Скопирован: {file}")
    
    # Копируем директории
    dirs_to_backup = [
        "artifacts",
        "app",
        "api", 
        "dashboard",
        "config",
        "adapters",
        "migration",
        "quality",
        "pipeline"
    ]
    
    for dir_name in dirs_to_backup:
        if Path(dir_name).exists():
            shutil.copytree(dir_name, backup_dir / dir_name, dirs_exist_ok=True)
            logger.info(f"Скопирована директория: {dir_name}")
    
    # Создаем информацию о бэкапе
    backup_info = {
        "timestamp": timestamp,
        "backup_dir": str(backup_dir),
        "files": files_to_backup,
        "directories": dirs_to_backup,
        "system_status": "working"
    }
    
    with open(backup_dir / "backup_info.json", "w", encoding="utf-8") as f:
        json.dump(backup_info, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Резервная копия создана: {backup_dir}")
    return backup_dir

def create_deployment_package():
    """Создание пакета для развертывания"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    package_dir = Path(f"deployment/dialogs_rag_system_{timestamp}")
    package_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Создание пакета развертывания в {package_dir}")
    
    # Основные файлы системы
    core_files = [
        "simple_api.py",
        "simple_dashboard.py",
        "analysis_summary.md",
        "README_ANALYSIS.md",
        "requirements_unified.txt",
        "Makefile_unified",
        "taxonomy.yaml",
        ".env.example"
    ]
    
    for file in core_files:
        if Path(file).exists():
            shutil.copy2(file, package_dir / file)
            logger.info(f"Добавлен в пакет: {file}")
    
    # Директории
    core_dirs = [
        "app",
        "config",
        "adapters", 
        "migration",
        "quality"
    ]
    
    for dir_name in core_dirs:
        if Path(dir_name).exists():
            shutil.copytree(dir_name, package_dir / dir_name, dirs_exist_ok=True)
            logger.info(f"Добавлена директория: {dir_name}")
    
    # Создаем скрипты запуска
    create_startup_scripts(package_dir)
    
    # Создаем README для развертывания
    create_deployment_readme(package_dir)
    
    logger.info(f"Пакет развертывания создан: {package_dir}")
    return package_dir

def create_startup_scripts(package_dir):
    """Создание скриптов запуска"""
    
    # Скрипт запуска для Unix/Linux/macOS
    start_script = """#!/bin/bash
# Скрипт запуска системы анализа диалогов

echo "🚀 Запуск системы анализа диалогов..."

# Проверяем Python
if ! command -v python &> /dev/null; then
    echo "❌ Python не найден. Установите Python 3.8+"
    exit 1
fi

# Устанавливаем зависимости
echo "📦 Установка зависимостей..."
pip install -r requirements_unified.txt

# Создаем необходимые директории
mkdir -p logs artifacts data/input data/processed data/duckdb

# Запускаем API
echo "🔌 Запуск API..."
python simple_api.py &
API_PID=$!

# Ждем запуска API
sleep 3

# Запускаем дашборд
echo "📊 Запуск дашборда..."
streamlit run simple_dashboard.py --server.port 8501 &
DASHBOARD_PID=$!

# Сохраняем PID процессов
echo $API_PID > api.pid
echo $DASHBOARD_PID > dashboard.pid

echo "✅ Система запущена!"
echo "API: http://localhost:8000"
echo "Дашборд: http://localhost:8501"
echo ""
echo "Для остановки выполните: ./stop.sh"
"""
    
    with open(package_dir / "start.sh", "w", encoding="utf-8") as f:
        f.write(start_script)
    
    # Делаем скрипт исполняемым
    os.chmod(package_dir / "start.sh", 0o755)
    
    # Скрипт остановки
    stop_script = """#!/bin/bash
# Скрипт остановки системы

echo "🛑 Остановка системы анализа диалогов..."

# Останавливаем API
if [ -f api.pid ]; then
    API_PID=$(cat api.pid)
    kill $API_PID 2>/dev/null
    rm api.pid
    echo "API остановлен"
fi

# Останавливаем дашборд
if [ -f dashboard.pid ]; then
    DASHBOARD_PID=$(cat dashboard.pid)
    kill $DASHBOARD_PID 2>/dev/null
    rm dashboard.pid
    echo "Дашборд остановлен"
fi

# Дополнительная очистка
pkill -f "python simple_api.py" 2>/dev/null
pkill -f "streamlit run simple_dashboard.py" 2>/dev/null

echo "✅ Система остановлена"
"""
    
    with open(package_dir / "stop.sh", "w", encoding="utf-8") as f:
        f.write(stop_script)
    
    os.chmod(package_dir / "stop.sh", 0o755)
    
    # Скрипт проверки статуса
    status_script = """#!/bin/bash
# Скрипт проверки статуса системы

echo "🔍 Проверка статуса системы..."

# Проверяем API
if curl -s http://localhost:8000/api/health > /dev/null; then
    echo "✅ API работает (http://localhost:8000)"
else
    echo "❌ API не отвечает"
fi

# Проверяем дашборд
if curl -s http://localhost:8501 > /dev/null; then
    echo "✅ Дашборд работает (http://localhost:8501)"
else
    echo "❌ Дашборд не отвечает"
fi

# Проверяем процессы
if pgrep -f "python simple_api.py" > /dev/null; then
    echo "✅ API процесс запущен"
else
    echo "❌ API процесс не найден"
fi

if pgrep -f "streamlit run simple_dashboard.py" > /dev/null; then
    echo "✅ Дашборд процесс запущен"
else
    echo "❌ Дашборд процесс не найден"
fi
"""
    
    with open(package_dir / "status.sh", "w", encoding="utf-8") as f:
        f.write(status_script)
    
    os.chmod(package_dir / "status.sh", 0o755)

def create_deployment_readme(package_dir):
    """Создание README для развертывания"""
    
    readme_content = """# 🚀 Система анализа диалогов - Пакет развертывания

## 📋 Требования

- Python 3.8+
- pip
- 2GB свободного места
- Порты 8000 и 8501 должны быть свободны

## 🚀 Быстрый старт

1. **Установка зависимостей:**
   ```bash
   pip install -r requirements_unified.txt
   ```

2. **Запуск системы:**
   ```bash
   ./start.sh
   ```

3. **Проверка статуса:**
   ```bash
   ./status.sh
   ```

4. **Остановка системы:**
   ```bash
   ./stop.sh
   ```

## 🌐 Доступ к системе

- **API:** http://localhost:8000
- **Дашборд:** http://localhost:8501
- **Документация API:** http://localhost:8000/docs

## 📊 Использование

### API Endpoints

```bash
# Статистика
curl http://localhost:8000/api/statistics

# Проблемы
curl http://localhost:8000/api/problems

# Идеи
curl http://localhost:8000/api/ideas

# Сигналы
curl http://localhost:8000/api/signals

# Поиск
curl "http://localhost:8000/api/search?query=доставка"
```

### Дашборд

Откройте http://localhost:8501 в браузере для интерактивного просмотра результатов.

## 📁 Структура пакета

```
dialogs_rag_system/
├── simple_api.py              # API сервер
├── simple_dashboard.py        # Дашборд
├── requirements_unified.txt   # Зависимости
├── start.sh                   # Скрипт запуска
├── stop.sh                    # Скрипт остановки
├── status.sh                  # Скрипт проверки
├── app/                       # Модули приложения
├── config/                    # Конфигурация
├── adapters/                  # Адаптеры данных
├── migration/                 # Инструменты миграции
└── quality/                   # Проверка качества
```

## 🔧 Настройка

1. **Переменные окружения:**
   ```bash
   cp .env.example .env
   # Отредактируйте .env файл
   ```

2. **Конфигурация:**
   - Основная конфигурация: `config/unified_config.yaml`
   - Таксономия: `taxonomy.yaml`

## 📈 Мониторинг

- **Логи API:** `logs/api.log`
- **Логи дашборда:** `logs/dashboard.log`
- **Проверка статуса:** `./status.sh`

## 🆘 Поддержка

При возникновении проблем:

1. Проверьте статус: `./status.sh`
2. Перезапустите систему: `./stop.sh && ./start.sh`
3. Проверьте логи в папке `logs/`

## 📋 Системные требования

- **ОС:** Linux, macOS, Windows
- **Python:** 3.8+
- **RAM:** 2GB+
- **Диск:** 2GB+
- **Сеть:** Порты 8000, 8501

---

**Версия:** 2.0.0  
**Дата создания:** """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """
"""
    
    with open(package_dir / "README_DEPLOYMENT.md", "w", encoding="utf-8") as f:
        f.write(readme_content)

def create_system_snapshot():
    """Создание снимка текущего состояния системы"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_file = f"system_snapshot_{timestamp}.json"
    
    # Собираем информацию о системе
    snapshot = {
        "timestamp": timestamp,
        "system_status": "running",
        "services": {
            "api": {
                "url": "http://localhost:8000",
                "status": "unknown"
            },
            "dashboard": {
                "url": "http://localhost:8501", 
                "status": "unknown"
            }
        },
        "files": {
            "api": "simple_api.py",
            "dashboard": "simple_dashboard.py",
            "requirements": "requirements_unified.txt",
            "makefile": "Makefile_unified"
        },
        "directories": [
            "app", "config", "adapters", "migration", "quality"
        ],
        "artifacts": {
            "comprehensive_results": "artifacts/comprehensive_results.json",
            "aggregate_results": "artifacts/aggregate_results.json",
            "statistics": "artifacts/statistics.json"
        }
    }
    
    # Проверяем статус сервисов
    try:
        import requests
        api_response = requests.get("http://localhost:8000/api/health", timeout=5)
        snapshot["services"]["api"]["status"] = "healthy" if api_response.status_code == 200 else "error"
    except:
        snapshot["services"]["api"]["status"] = "unavailable"
    
    try:
        dashboard_response = requests.get("http://localhost:8501", timeout=5)
        snapshot["services"]["dashboard"]["status"] = "healthy" if dashboard_response.status_code == 200 else "error"
    except:
        snapshot["services"]["dashboard"]["status"] = "unavailable"
    
    # Сохраняем снимок
    with open(snapshot_file, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Снимок системы создан: {snapshot_file}")
    return snapshot_file

def main():
    """Главная функция сохранения системы"""
    logger.info("💾 Сохранение системы анализа диалогов...")
    
    # Создаем снимок текущего состояния
    snapshot_file = create_system_snapshot()
    logger.info(f"✅ Снимок состояния: {snapshot_file}")
    
    # Создаем резервную копию
    backup_dir = create_backup()
    logger.info(f"✅ Резервная копия: {backup_dir}")
    
    # Создаем пакет развертывания
    package_dir = create_deployment_package()
    logger.info(f"✅ Пакет развертывания: {package_dir}")
    
    # Создаем архив пакета
    archive_name = f"dialogs_rag_system_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tar.gz"
    subprocess.run([
        "tar", "-czf", archive_name, "-C", str(package_dir.parent), package_dir.name
    ], check=True)
    
    logger.info(f"✅ Архив создан: {archive_name}")
    
    # Создаем итоговый отчет
    report = f"""
# 💾 Система анализа диалогов сохранена

**Дата сохранения:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📦 Созданные файлы:

1. **Снимок состояния:** {snapshot_file}
2. **Резервная копия:** {backup_dir}
3. **Пакет развертывания:** {package_dir}
4. **Архив:** {archive_name}

## 🚀 Для развертывания:

```bash
# Распакуйте архив
tar -xzf {archive_name}

# Перейдите в директорию
cd {package_dir.name}

# Запустите систему
./start.sh
```

## 📊 Текущий статус системы:

- **API:** http://localhost:8000
- **Дашборд:** http://localhost:8501
- **Статус:** Работает

## 🔧 Управление:

- **Запуск:** `./start.sh`
- **Остановка:** `./stop.sh`
- **Статус:** `./status.sh`

---
Система готова к использованию! 🎉
"""
    
    with open("SAVE_REPORT.md", "w", encoding="utf-8") as f:
        f.write(report)
    
    logger.info("✅ Отчет сохранения: SAVE_REPORT.md")
    logger.info("🎉 Система успешно сохранена!")
    
    print("\n" + "="*60)
    print("💾 СИСТЕМА АНАЛИЗА ДИАЛОГОВ СОХРАНЕНА")
    print("="*60)
    print(f"📸 Снимок состояния: {snapshot_file}")
    print(f"💾 Резервная копия: {backup_dir}")
    print(f"📦 Пакет развертывания: {package_dir}")
    print(f"🗜️  Архив: {archive_name}")
    print(f"📋 Отчет: SAVE_REPORT.md")
    print("\n🌐 Текущий статус:")
    print("   API: http://localhost:8000")
    print("   Дашборд: http://localhost:8501")
    print("\n🚀 Для развертывания на другом сервере:")
    print(f"   tar -xzf {archive_name}")
    print(f"   cd {package_dir.name}")
    print("   ./start.sh")
    print("="*60)

if __name__ == "__main__":
    main()
