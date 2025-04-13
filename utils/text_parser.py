#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для парсинга неструктурированного текста сельскохозяйственных данных.

Этот модуль предоставляет функции для извлечения структурированной информации
из текстовых сообщений о сельскохозяйственных операциях. Он обрабатывает:
- Даты проведения операций
- Типы сельскохозяйственных операций
- Культуры
- Подразделения и отделы
- Метрики (площади, урожайность)

Основные компоненты:
- Регулярные выражения для поиска паттернов
- Функции парсинга различных элементов данных
- Функции валидации и коррекции данных

Пример использования:
    from utils.text_parser import parse_messages
    
    input_data = {
        "messages": [
            {
                "id": 1,
                "date": "2025-04-12",
                "payload": "Пахота зяби под мн тр\\nПо Пу 26/488\\nОтд 12 26/221"
            }
        ]
    }
    
    parsed_data = parse_messages(input_data)
"""

import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from utils.input_processor import prepare_messages
from config.reference_data import (
    VALID_OPERATIONS,
    DIVISIONS,
    VALID_CROPS,
    correct_operation,
    correct_crop,
    expand_abbreviation
)

logger = logging.getLogger(__name__)

# Регулярные выражения для парсинга
DATE_PATTERN = r'(\d{1,2})[\./](\d{1,2})(?:[\./](\d{4}))?'  # Даты в формате DD.MM или DD.MM.YYYY
ISO_DATE_PATTERN = r'(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})'   # Даты в ISO формате YYYY-MM-DD
OPERATION_PATTERN = r'^([А-Яа-я\s\-]+)(?:\s+под\s+([А-Яа-я\s]+))?'  # Операция и культура
DEPARTMENT_PATTERN = r'Отд\s+(\d+)\s+(\d+)/(\d+)'  # Отдел и метрики
PRODUCTION_UNIT_PATTERN = r'По\s+([А-Яа-я]+)\s+(\d+)/(\d+)'  # Производственный участок и метрики
METRICS_PATTERN = r'(\d+)/(\d+)'  # Метрики в формате daily/total

# Словарь соответствия отделов подразделениям
DEPARTMENT_TO_DIVISION = {
    range(1, 11): "АОР-1",
    range(11, 21): "АОР-2",
    range(21, 31): "АОР-3"
}

def get_division_from_department(dept_num: int) -> Optional[str]:
    """
    Определение подразделения по номеру отдела.
    
    Args:
        dept_num (int): Номер отдела
        
    Returns:
        Optional[str]: Название подразделения или None
    """
    try:
        for dept_range, division in DEPARTMENT_TO_DIVISION.items():
            if dept_num in dept_range:
                return division
        return None
    except Exception as e:
        logger.error(f"Ошибка при определении подразделения: {str(e)}")
        return None

def parse_messages(input_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Парсинг всех сообщений во входных данных.
    
    Эта функция является основной точкой входа для парсинга данных. Она:
    1. Подготавливает сообщения к обработке
    2. Обрабатывает каждое сообщение отдельно
    3. Собирает результаты в единый список
    
    Args:
        input_data (Dict[str, List[Dict[str, Any]]]): Словарь с входными данными.
            Ожидаемая структура:
            {
                "messages": [
                    {
                        "id": int,
                        "date": str,
                        "payload": str
                    },
                    ...
                ]
            }
        
    Returns:
        List[Dict[str, Any]]: Список обработанных сообщений с извлеченными данными.
            Каждое сообщение содержит:
            - Исходный payload
            - Список распарсенных операций
            - Метаданные сообщения
        
    Raises:
        ValueError: Если входные данные имеют неверный формат
        Exception: При других ошибках парсинга
    """
    try:
        # Подготовка сообщений
        messages = prepare_messages(input_data)
        parsed_messages = []
        
        for message in messages:
            # Парсинг payload
            parsed_operations = parse_message_payload(
                message['payload'],
                date=message.get('date')
            )
            
            # Обновление сообщения результатами парсинга
            message['parsed'] = parsed_operations
            parsed_messages.append(message)
            
        logger.info(f"Успешно распарсено {len(parsed_messages)} сообщений")
        return parsed_messages
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге сообщений: {str(e)}")
        raise

def parse_message_payload(payload: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Парсинг текста сообщения.
    
    Функция разбирает текст сообщения на составляющие:
    1. Извлекает дату (если не предоставлена)
    2. Разделяет текст на блоки операций
    3. Парсит каждый блок отдельно
    
    Args:
        payload (str): Текст сообщения для парсинга
        date (Optional[str]): Дата сообщения (если известна)
        
    Returns:
        List[Dict[str, Any]]: Список распарсенных операций.
            Каждая операция содержит:
            - date: str - Дата операции
            - operation: str - Тип операции
            - crop: Optional[str] - Культура
            - division: Optional[str] - Подразделение
            - department: Optional[int] - Номер отдела
            - dailyArea: int - Дневная площадь
            - totalArea: int - Общая площадь
            
    Example:
        >>> payload = "Пахота зяби под мн тр\\nПо Пу 26/488\\nОтд 12 26/221"
        >>> parse_message_payload(payload, "2025-04-12")
        [
            {
                'date': '12.04',
                'operation': 'Пахота зяби',
                'crop': 'Многолетние травы',
                'division': 'АОР',
                'department': 12,
                'dailyArea': 26,
                'totalArea': 221
            },
            ...
        ]
    """
    try:
        # Извлечение даты если не предоставлена
        message_date = None
        if date:
            message_date = parse_date(date)
        if not message_date:
            # Попробуем найти дату в тексте
            date_match = re.search(DATE_PATTERN, payload)
            if date_match:
                message_date = parse_date(date_match.group(0))
                
        # Разделение на блоки операций
        operation_blocks = split_into_operation_blocks(payload)
        parsed_operations = []
        
        for block in operation_blocks:
            operations = parse_operation_block(block, message_date)
            parsed_operations.extend(operations)
            
        return parsed_operations
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге сообщения: {str(e)}")
        return []

def split_into_operation_blocks(payload: str) -> List[str]:
    """
    Разделение текста на блоки операций.
    
    Args:
        payload (str): Текст сообщения
        
    Returns:
        List[str]: Список блоков операций
    """
    # Разделение по пустым строкам
    blocks = re.split(r'\n\s*\n', payload.strip())
    
    # Фильтрация пустых блоков
    return [block.strip() for block in blocks if block.strip()]

def parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Парсинг даты из строки.
    
    Args:
        date_str (Optional[str]): Строка с датой
        
    Returns:
        Optional[str]: Дата в формате DD.MM или None
    """
    if not date_str:
        return None
        
    try:
        # Пробуем ISO формат (YYYY-MM-DD)
        iso_match = re.match(ISO_DATE_PATTERN, date_str)
        if iso_match:
            year, month, day = map(int, iso_match.groups())
            return f"{day:02d}.{month:02d}"
            
        # Пробуем стандартный формат (DD.MM или DD.MM.YYYY)
        match = re.match(DATE_PATTERN, date_str)
        if match:
            day, month, year = match.groups()
            day = int(day)
            month = int(month)
            
            # Проверяем корректность даты
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{day:02d}.{month:02d}"
                
        logger.warning(f"Не удалось распознать формат даты: {date_str}")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге даты {date_str}: {str(e)}")
        return None

def parse_operation_block(block: str, date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Парсинг блока операции.
    
    Args:
        block (str): Блок текста с операцией
        date (Optional[str]): Дата операции
        
    Returns:
        List[Dict[str, Any]]: Список распарсенных операций
    """
    try:
        # Извлечение основной информации
        operation_match = re.match(OPERATION_PATTERN, block)
        if not operation_match:
            logger.warning(f"Не удалось распознать операцию в блоке: {block}")
            return []
            
        operation, crop = operation_match.groups()
        operation = correct_operation(operation)
        
        # Поиск культуры в тексте
        crop_patterns = [
            r'под\s+([А-Яа-я\s\.]+?)(?:\s+|$)',  # после "под"
            r'по\s+([А-Яа-я\s\.]+?)(?:\s+|$)',   # после "по"
            r'([А-Яа-я\s\.]+?)\s+(?:Отд|По)',    # перед "Отд" или "По"
        ]
        
        found_crop = None
        for pattern in crop_patterns:
            crop_match = re.search(pattern, block)
            if crop_match:
                found_crop = crop_match.group(1).strip()
                found_crop = correct_crop(found_crop)
                if found_crop:
                    break
                    
        # Если культура не найдена, используем значение из operation_match
        if not found_crop and crop:
            found_crop = correct_crop(crop)
        
        # Поиск данных по отделам
        department_matches = re.finditer(DEPARTMENT_PATTERN, block)
        operations = []
        
        for match in department_matches:
            dept_num, daily_area, total_area = map(int, match.groups())
            
            operation_data = {
                'date': date,
                'operation': operation,
                'crop': found_crop,
                'department': dept_num,
                'dailyArea': daily_area,
                'totalArea': total_area,
                'dailyYield': None,
                'totalYield': None
            }
            
            # Добавление информации о подразделении
            division = get_division_from_department(dept_num)
            if division:
                operation_data['division'] = division
                
            operations.append(operation_data)
            
        # Поиск данных по производственным участкам
        pu_matches = re.finditer(PRODUCTION_UNIT_PATTERN, block)
        for match in pu_matches:
            pu_name, daily_area, total_area = match.groups()
            
            operation_data = {
                'date': date,
                'operation': operation,
                'crop': found_crop,
                'productionUnit': pu_name,
                'dailyArea': int(daily_area),
                'totalArea': int(total_area),
                'dailyYield': None,
                'totalYield': None
            }
            
            operations.append(operation_data)
            
        return operations
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге блока операции: {str(e)}")
        return []

def extract_operation(text: str) -> Optional[str]:
    """
    Извлечение типа операции из текста.
    
    Args:
        text (str): Текст с операцией
        
    Returns:
        Optional[str]: Тип операции или None
    """
    match = re.match(OPERATION_PATTERN, text)
    if match:
        operation = match.group(1).strip()
        return correct_operation(operation)
    return None

def extract_crop(text: str) -> Optional[str]:
    """
    Извлечение культуры из текста.
    
    Args:
        text (str): Текст с культурой
        
    Returns:
        Optional[str]: Название культуры или None
    """
    match = re.match(OPERATION_PATTERN, text)
    if match and match.group(2):
        crop = match.group(2).strip()
        return correct_crop(crop)
    return None

def extract_division(text: str) -> Optional[str]:
    """
    Извлечение подразделения из текста.
    
    Args:
        text (str): Текст с подразделением
        
    Returns:
        Optional[str]: Название подразделения или None
    """
    for division in DIVISIONS:
        if division.lower() in text.lower():
            return division
    return None

def extract_metrics(text: str) -> Optional[Tuple[int, int]]:
    """
    Извлечение метрик из текста.
    
    Args:
        text (str): Текст с метриками
        
    Returns:
        Optional[Tuple[int, int]]: Кортеж (daily, total) или None
    """
    match = re.search(METRICS_PATTERN, text)
    if match:
        daily, total = map(int, match.groups())
        return daily, total
    return None 