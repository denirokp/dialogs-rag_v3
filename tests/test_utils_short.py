#!/usr/bin/env python3
"""
Микро-тесты для утилит
"""

import sys
from pathlib import Path

# Добавляем корневую папку в путь
sys.path.append(str(Path(__file__).parent.parent))

from pipeline.stage4_cluster import auto_label_cluster
from pipeline.stage2_extract_entities import guess_product_category


def test_auto_label_cluster_skips_trash():
    """Тест: авто-лейбл кластера пропускает мусорные слова"""
    label = auto_label_cluster(["Неэффективность платформы", "мало пвз", "вопрос"])
    assert label.lower().startswith("мало пвз")


def test_guess_product_category():
    """Тест: определение категории товара"""
    assert guess_product_category("Продам зимние шины Nokian 225/55") == "шины"
    assert guess_product_category("Кирпич облицовочный, цемент м500") == "стройматериалы"
    assert guess_product_category("Продам диван угловой") == "мебель"
    assert guess_product_category("Смартфон iPhone 15 Pro Max") == "электроника"
    assert guess_product_category("Запчасти для ВАЗ") == "автозапчасти"
    assert guess_product_category("Обычный товар") is None


def test_auto_label_cluster_fallback():
    """Тест: fallback для авто-лейбла кластера"""
    # Все варианты - мусор
    label = auto_label_cluster(["вопрос", "проблема", "технические"])
    assert label == "Кластер"
    
    # Пустой список
    label = auto_label_cluster([])
    assert label == "Кластер"


def test_auto_label_cluster_normalization():
    """Тест: нормализация в авто-лейбле кластера"""
    label = auto_label_cluster(["  Дорогая  доставка  ", "дорогая доставка", "дорогая доставка"])
    assert label == "Дорогая доставка"


if __name__ == "__main__":
    # Запуск тестов
    test_auto_label_cluster_skips_trash()
    print("✅ test_auto_label_cluster_skips_trash passed")
    
    test_guess_product_category()
    print("✅ test_guess_product_category passed")
    
    test_auto_label_cluster_fallback()
    print("✅ test_auto_label_cluster_fallback passed")
    
    test_auto_label_cluster_normalization()
    print("✅ test_auto_label_cluster_normalization passed")
    
    print("\n🎉 Все микро-тесты прошли успешно!")
