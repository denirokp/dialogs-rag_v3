#!/usr/bin/env python3
"""
🎯 ГЛАВНЫЙ ФАЙЛ DIALOGS RAG SYSTEM
Единая точка входа для всех функций системы
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List, Any

# Добавляем пути для импорта
sys.path.append(str(Path(__file__).parent))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/main.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def print_banner():
    """Печать баннера системы"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                    DIALOGS RAG SYSTEM                        ║
    ║                                                              ║
    ║  🎯 Анализ диалогов с извлечением барьеров, идей и сигналов  ║
    ║  🧠 Самообучение и адаптация                                ║
    ║  📊 Детальные отчеты и визуализация                         ║
    ║  🔧 Полная автоматизация процесса                           ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def run_full_pipeline(input_file: str, config_file: str = "config.json"):
    """Запуск полного пайплайна"""
    logger.info("🚀 Запуск полного пайплайна...")
    
    try:
        from pipeline_manager import PipelineManager, PipelineConfig
        
        config = PipelineConfig()
        manager = PipelineManager(config)
        
        # Запуск всех этапов
        success = manager.run_pipeline(["1", "1.5", "2", "3", "4", "5", "6", "7"])
        
        if success:
            logger.info("✅ Полный пайплайн завершен успешно!")
            return True
        else:
            logger.error("❌ Ошибка в полном пайплайне")
            return False
            
    except Exception as e:
        logger.error(f"❌ Ошибка запуска полного пайплайна: {e}")
        return False

def run_comprehensive_pipeline(input_file: str, config_file: str = "config.json"):
    """Запуск комплексного DoD пайплайна"""
    logger.info("🎯 Запуск комплексного DoD пайплайна...")
    
    try:
        from comprehensive_dod_pipeline import main as comprehensive_main
        
        # Запуск с аргументами
        sys.argv = ['comprehensive_dod_pipeline.py', '--input', input_file, '--config', config_file]
        asyncio.run(comprehensive_main())
        
        logger.info("✅ Комплексный DoD пайплайн завершен успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска комплексного пайплайна: {e}")
        return False

def run_enhanced_pipeline(input_file: str, config_file: str = "config.json"):
    """Запуск расширенного пайплайна с самообучением"""
    logger.info("🧠 Запуск расширенного пайплайна с самообучением...")
    
    try:
        from enhanced_main import main as enhanced_main
        
        # Запуск с аргументами
        sys.argv = ['enhanced_main.py', '--input', input_file, '--config', config_file, '--enable-all']
        asyncio.run(enhanced_main())
        
        logger.info("✅ Расширенный пайплайн завершен успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска расширенного пайплайна: {e}")
        return False

def run_api_server():
    """Запуск API сервера"""
    logger.info("🌐 Запуск API сервера...")
    
    try:
        from api.pipeline_api import app
        import uvicorn
        
        uvicorn.run(app, host="0.0.0.0", port=8000)
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска API сервера: {e}")

def run_dashboard():
    """Запуск дашборда"""
    logger.info("📊 Запуск дашборда...")
    
    try:
        from dashboard.pipeline_dashboard import main as dashboard_main
        dashboard_main()
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска дашборда: {e}")

def run_tests():
    """Запуск тестов"""
    logger.info("🧪 Запуск тестов...")
    
    try:
        from final_system_test import main as test_main
        test_main()
        
        logger.info("✅ Все тесты пройдены успешно!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска тестов: {e}")
        return False

def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Dialogs RAG System - Главный файл')
    parser.add_argument('--mode', '-m', 
                       choices=['full', 'comprehensive', 'enhanced', 'api', 'dashboard', 'test'],
                       default='comprehensive',
                       help='Режим работы системы')
    parser.add_argument('--input', '-i', 
                       default='data/dialogs.xlsx',
                       help='Путь к файлу с диалогами')
    parser.add_argument('--config', '-c', 
                       default='config.json',
                       help='Путь к файлу конфигурации')
    
    args = parser.parse_args()
    
    # Печать баннера
    print_banner()
    
    # Создание необходимых директорий
    Path('logs').mkdir(exist_ok=True)
    Path('artifacts').mkdir(exist_ok=True)
    Path('reports').mkdir(exist_ok=True)
    
    # Выбор режима работы
    if args.mode == 'full':
        success = run_full_pipeline(args.input, args.config)
    elif args.mode == 'comprehensive':
        success = run_comprehensive_pipeline(args.input, args.config)
    elif args.mode == 'enhanced':
        success = run_enhanced_pipeline(args.input, args.config)
    elif args.mode == 'api':
        run_api_server()
        return
    elif args.mode == 'dashboard':
        run_dashboard()
        return
    elif args.mode == 'test':
        success = run_tests()
    else:
        logger.error(f"❌ Неизвестный режим: {args.mode}")
        return
    
    if success:
        logger.info("🎉 Система завершена успешно!")
    else:
        logger.error("❌ Система завершена с ошибками")
        sys.exit(1)

if __name__ == "__main__":
    main()
