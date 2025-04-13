#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Главный модуль приложения для обработки сельскохозяйственных данных.
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, Any, List, Optional

from utils.input_processor import load_input_json
from utils.text_parser import parse_messages
from utils.validator import validate_parsed_data
from utils.error_handler import handle_errors
from utils.output_formatter import format_output

def setup_logging(verbose: bool = False) -> None:
    """
    Настройка системы логирования с консольным и файловым выводом.
    
    Args:
        verbose (bool): Включить подробное логирование
    """
    # Создаем форматтер для логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Очищаем существующие обработчики
    root_logger.handlers = []
    
    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    root_logger.addHandler(console_handler)
    
    # Файловый обработчик
    file_handler = logging.FileHandler('app.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)  # Всегда пишем подробные логи в файл
    root_logger.addHandler(file_handler)
    
    # Отключаем логи от внешних библиотек
    for logger_name in ['urllib3', 'chardet', 'charset_normalizer']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

def setup_argument_parser() -> argparse.ArgumentParser:
    """
    Настройка парсера аргументов командной строки.
    
    Returns:
        argparse.ArgumentParser: Настроенный парсер аргументов
    """
    parser = argparse.ArgumentParser(
        description='Обработка сельскохозяйственных данных из JSON файла'
    )
    parser.add_argument(
        'input_file',
        help='Путь к входному JSON файлу'
    )
    parser.add_argument(
        '-o', '--output',
        default='output.json',
        help='Путь к выходному JSON файлу (по умолчанию: output.json)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Включить подробное логирование'
    )
    return parser

def process_data(input_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Обработка входных данных через весь конвейер.
    
    Args:
        input_data (Dict[str, List[Dict[str, Any]]]): Входные данные
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Обработанные данные
        
    Raises:
        ValueError: Если входные данные некорректны или None
    """
    if input_data is None:
        raise ValueError("Входные данные не могут быть None")
        
    logger = logging.getLogger(__name__)
    try:
        # 1. Парсинг сообщений
        logger.info("Начало парсинга сообщений...")
        parsed_data = parse_messages(input_data)
        logger.info(f"Успешно распарсено {len(parsed_data)} сообщений")
        
        # 2. Валидация данных
        logger.info("Начало валидации данных...")
        validated_data = validate_parsed_data(parsed_data)
        logger.info(f"Успешно валидировано {len(validated_data)} сообщений")
        
        # 3. Обработка ошибок
        logger.info("Начало обработки ошибок...")
        corrected_data = handle_errors(validated_data)
        logger.info(f"Успешно обработано {len(corrected_data)} сообщений")
        
        # 4. Форматирование вывода
        logger.info("Форматирование выходных данных...")
        formatted_data = format_output(corrected_data)
        logger.info("Форматирование завершено")
        
        return formatted_data
        
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {str(e)}", exc_info=True)
        raise

def print_processing_summary(data: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Вывод сводки по обработанным данным.
    
    Args:
        data (Dict[str, List[Dict[str, Any]]]): Обработанные данные
    """
    logger = logging.getLogger(__name__)
    logger.info("\n=== Сводка по обработанным данным ===")
    
    total_messages = len(data["reports"])
    total_operations = sum(len(report["parsed"]) for report in data["reports"])
    
    logger.info(f"Всего обработано сообщений: {total_messages}")
    logger.info(f"Всего извлечено операций: {total_operations}")
    
    # Статистика по типам операций
    operation_stats = {}
    for report in data["reports"]:
        for operation in report["parsed"]:
            op_type = operation.get("operation", "Неизвестно")
            operation_stats[op_type] = operation_stats.get(op_type, 0) + 1
            
    logger.info("\nСтатистика по операциям:")
    for op_type, count in operation_stats.items():
        logger.info(f"  {op_type}: {count}")
        
    # Проверка на пропущенные данные
    missing_data = {
        "date": 0,
        "division": 0,
        "operation": 0,
        "crop": 0,
        "dailyArea": 0,
        "totalArea": 0
    }
    
    for report in data["reports"]:
        for operation in report["parsed"]:
            for field in missing_data:
                if not operation.get(field):
                    missing_data[field] += 1
                    
    logger.info("\nПропущенные данные:")
    for field, count in missing_data.items():
        if count > 0:
            logger.warning(f"  {field}: {count} пропусков")
            
    logger.info("\n=== Конец сводки ===\n")

def main():
    """Основная функция приложения."""
    try:
        # Парсинг аргументов командной строки
        parser = setup_argument_parser()
        args = parser.parse_args()
        
        # Настройка логирования
        setup_logging(args.verbose)
        logger = logging.getLogger(__name__)
        
        # Проверка существования входного файла
        if not os.path.exists(args.input_file):
            logger.error(f"Входной файл не найден: {args.input_file}")
            sys.exit(1)
            
        # Загрузка входных данных
        logger.info(f"Загрузка данных из файла: {args.input_file}")
        input_data = load_input_json(args.input_file)
        
        # Обработка данных
        processed_data = process_data(input_data)
        if not processed_data:
            logger.error("Не удалось обработать данные")
            sys.exit(1)
            
        # Сохранение результатов
        logger.info(f"Сохранение результатов в файл: {args.output}")
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
            
        # Вывод сводки
        print_processing_summary(processed_data)
        
        logger.info("Обработка успешно завершена")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main() 