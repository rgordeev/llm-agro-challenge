import unittest
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from main import process_agricultural_data, main

class TestAgriculturalDataProcessor(unittest.TestCase):
    def setUp(self):
        """Подготовка тестовых данных"""
        self.test_data = {
            "messages": [
                {
                    "id": 1,
                    "date": "2025-04-12",
                    "payload": "Пахота зяби под мн тр\nПо Пу 26/488\nОтд 12 26/221"
                }
            ]
        }
        
        # Создаем временный файл для тестов
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_input_path = Path(self.temp_dir.name) / "test_input.json"
        self.test_output_path = Path(self.temp_dir.name) / "test_output.json"
        
        # Записываем тестовые данные во временный файл
        with open(self.test_input_path, 'w', encoding='utf-8') as f:
            json.dump(self.test_data, f, ensure_ascii=False)

    def tearDown(self):
        """Очистка после тестов"""
        self.temp_dir.cleanup()

    def test_process_agricultural_data(self):
        """Тест функции process_agricultural_data"""
        result = process_agricultural_data(self.test_data)
        
        # Проверяем структуру результата
        self.assertIn("status", result)
        self.assertIn("data", result)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["data"], self.test_data)

    def test_process_agricultural_data_with_invalid_input(self):
        """Тест обработки некорректных данных"""
        with self.assertRaises(Exception):
            process_agricultural_data(None)

    @patch('sys.argv', ['main.py', 'tests/test_data.json', '--output', 'output.json'])
    def test_main_success(self):
        """Тест успешного выполнения main"""
        with patch('builtins.open', mock_open(read_data=json.dumps(self.test_data))):
            with patch('json.dump') as mock_dump:
                main()
                mock_dump.assert_called_once()

    @patch('sys.argv', ['main.py', 'nonexistent_file.json'])
    def test_main_file_not_found(self):
        """Тест обработки отсутствующего файла"""
        with self.assertRaises(SystemExit) as cm:
            main()
        self.assertEqual(cm.exception.code, 1)

    @patch('sys.argv', ['main.py', 'tests/test_data.json'])
    def test_main_invalid_json(self):
        """Тест обработки некорректного JSON"""
        with patch('builtins.open', mock_open(read_data='invalid json')):
            with self.assertRaises(SystemExit) as cm:
                main()
            self.assertEqual(cm.exception.code, 1)

if __name__ == '__main__':
    unittest.main() 