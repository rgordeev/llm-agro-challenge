#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тесты для конвейера обработки данных.
"""

import os
import json
import logging
import unittest
from typing import Dict, Any, List

from utils.input_processor import load_input_json as load_input
from utils.text_parser import parse_messages
from utils.validator import validate_parsed_data
from utils.error_handler import handle_errors
from utils.output_formatter import format_output

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestDataProcessingPipeline(unittest.TestCase):
    """Тесты для конвейера обработки данных."""
    
    def setUp(self):
        """Подготовка тестовых данных."""
        self.test_input_file = "test_input.json"
        self.test_output_file = "test_output.json"
        
        # Создание тестовых данных
        self.sample_data = {
            "messages": [
                {
                    "id": 1,
                    "date": "2025-04-12",
                    "payload": """Пахота зяби под мн тр
По Пу 26/488
Отд 12 26/221
Отд 13 26/221
Отд 14 26/221
Отд 15 26/221
Отд 16 26/221
Отд 17 26/221
Отд 18 26/221
Отд 19 26/221
Отд 20 26/221
Отд 21 26/221
Отд 22 26/221
Отд 23 26/221
Отд 24 26/221
Отд 25 26/221
Отд 26 26/221
Отд 27 26/221
Отд 28 26/221
Отд 29 26/221
Отд 30 26/221"""
                },
                {
                    "id": 2,
                    "date": "2025-04-13",
                    "payload": """Сев озимой пшеницы
По АОР 15/300
Отд 1 15/300
Отд 2 15/300
Отд 3 15/300
Отд 4 15/300
Отд 5 15/300
Отд 6 15/300
Отд 7 15/300
Отд 8 15/300
Отд 9 15/300
Отд 10 15/300
Отд 11 15/300
Отд 12 15/300
Отд 13 15/300
Отд 14 15/300
Отд 15 15/300"""
                }
            ]
        }
        
        # Сохранение тестовых данных в файл
        with open(self.test_input_file, 'w', encoding='utf-8') as f:
            json.dump(self.sample_data, f, ensure_ascii=False, indent=2)
            
    def tearDown(self):
        """Очистка после тестов."""
        if os.path.exists(self.test_input_file):
            os.remove(self.test_input_file)
        if os.path.exists(self.test_output_file):
            os.remove(self.test_output_file)
            
    def test_full_pipeline(self):
        """Тест полного конвейера обработки данных."""
        try:
            # 1. Загрузка входных данных
            input_data = load_input(self.test_input_file)
            self.assertIsNotNone(input_data)
            self.assertIn("messages", input_data)
            
            # 2. Парсинг сообщений
            parsed_data = parse_messages({"messages": input_data["messages"]})
            self.assertIsNotNone(parsed_data)
            self.assertGreater(len(parsed_data), 0)
            
            # 3. Валидация данных
            validated_data = validate_parsed_data(parsed_data)
            self.assertIsNotNone(validated_data)
            self.assertGreater(len(validated_data), 0)
            
            # 4. Обработка ошибок
            corrected_data = handle_errors(validated_data)
            self.assertIsNotNone(corrected_data)
            self.assertGreater(len(corrected_data), 0)
            
            # 5. Форматирование вывода
            formatted_data = format_output(corrected_data)
            self.assertIsNotNone(formatted_data)
            self.assertIn("reports", formatted_data)
            
            # Сохранение результатов
            with open(self.test_output_file, 'w', encoding='utf-8') as f:
                json.dump(formatted_data, f, ensure_ascii=False, indent=2)
                
            # Вывод сводки
            self._print_summary(formatted_data)
            
        except Exception as e:
            self.fail(f"Ошибка в конвейере обработки данных: {str(e)}")
            
    def _print_summary(self, data: Dict[str, List[Dict[str, Any]]]):
        """Вывод сводки по обработанным данным."""
        logger.info("\n=== Сводка по обработанным данным ===")
        
        for report in data["reports"]:
            logger.info(f"\nСообщение #{report['message_number']}")
            logger.info(f"Текст: {report['payload'][:100]}...")
            
            for operation in report["parsed"]:
                logger.info("\nОперация:")
                logger.info(f"  Дата: {operation.get('date', 'Нет')}")
                logger.info(f"  Подразделение: {operation.get('division', 'Нет')}")
                logger.info(f"  Операция: {operation.get('operation', 'Нет')}")
                logger.info(f"  Культура: {operation.get('crop', 'Нет')}")
                logger.info(f"  Дневная площадь: {operation.get('dailyArea', 'Нет')}")
                logger.info(f"  Общая площадь: {operation.get('totalArea', 'Нет')}")
                logger.info(f"  Дневная урожайность: {operation.get('dailyYield', 'Нет')}")
                logger.info(f"  Общая урожайность: {operation.get('totalYield', 'Нет')}")
                
        logger.info("\n=== Конец сводки ===\n")

if __name__ == '__main__':
    unittest.main() 