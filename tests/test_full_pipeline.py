import unittest
import json
import os
from main import process_data
from utils.input_processor import load_data
from utils.text_parser import parse_messages
from utils.validator import validate_parsed_data

class TestFullPipeline(unittest.TestCase):
    def setUp(self):
        """Подготовка тестовых данных"""
        self.input_file = "tests/input.json"
        self.expected_output_file = "tests/expected_output.json"
        
        # Загружаем ожидаемый результат
        with open(self.expected_output_file, 'r', encoding='utf-8') as f:
            self.expected_output = json.load(f)

    def test_full_pipeline(self):
        """Тест полного пайплайна обработки данных"""
        # Загружаем входные данные
        input_data = load_data(self.input_file)
        
        # Парсим сообщения
        parsed_messages = parse_messages(input_data)
        
        # Валидируем данные
        validated_messages = validate_parsed_data(parsed_messages)
        
        # Формируем результат
        result = {
            "reports": [
                {
                    "message_number": msg["id"],
                    "payload": msg["payload"],
                    "parsed": msg["parsed"]
                }
                for msg in validated_messages
            ]
        }
        
        # Сравниваем с ожидаемым результатом
        self.assertEqual(len(result["reports"]), len(self.expected_output["reports"]))
        
        for actual_report, expected_report in zip(result["reports"], self.expected_output["reports"]):
            # Проверяем номер сообщения
            self.assertEqual(actual_report["message_number"], expected_report["message_number"])
            
            # Проверяем payload
            self.assertEqual(actual_report["payload"], expected_report["payload"])
            
            # Проверяем количество операций
            self.assertEqual(len(actual_report["parsed"]), len(expected_report["parsed"]))
            
            # Проверяем каждую операцию
            for actual_op, expected_op in zip(actual_report["parsed"], expected_report["parsed"]):
                self.assertEqual(actual_op["date"], expected_op["date"])
                self.assertEqual(actual_op["division"], expected_op["division"])
                self.assertEqual(actual_op["operation"], expected_op["operation"])
                self.assertEqual(actual_op["crop"], expected_op["crop"])
                self.assertEqual(actual_op["dailyArea"], expected_op["dailyArea"])
                self.assertEqual(actual_op["totalArea"], expected_op["totalArea"])
                self.assertEqual(actual_op["dailyYield"], expected_op["dailyYield"])
                self.assertEqual(actual_op["totalYield"], expected_op["totalYield"])

    def test_process_data_function(self):
        """Тест функции process_data из main.py"""
        result = process_data(self.input_file)
        
        # Проверяем структуру результата
        self.assertIn("reports", result)
        self.assertEqual(len(result["reports"]), len(self.expected_output["reports"]))
        
        # Проверяем каждое сообщение
        for actual_report, expected_report in zip(result["reports"], self.expected_output["reports"]):
            self.assertEqual(actual_report["message_number"], expected_report["message_number"])
            self.assertEqual(actual_report["payload"], expected_report["payload"])
            self.assertEqual(len(actual_report["parsed"]), len(expected_report["parsed"]))
            
            for actual_op, expected_op in zip(actual_report["parsed"], expected_report["parsed"]):
                self.assertEqual(actual_op["date"], expected_op["date"])
                self.assertEqual(actual_op["division"], expected_op["division"])
                self.assertEqual(actual_op["operation"], expected_op["operation"])
                self.assertEqual(actual_op["crop"], expected_op["crop"])
                self.assertEqual(actual_op["dailyArea"], expected_op["dailyArea"])
                self.assertEqual(actual_op["totalArea"], expected_op["totalArea"])
                self.assertEqual(actual_op["dailyYield"], expected_op["dailyYield"])
                self.assertEqual(actual_op["totalYield"], expected_op["totalYield"])

if __name__ == '__main__':
    unittest.main() 