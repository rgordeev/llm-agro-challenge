#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для парсинга неструктурированного текста сельскохозяйственных данных.
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
DATE_PATTERN = r'(\d{1,2})[\./](\d{1,2})(?:[\./](\d{4}))?'
OPERATION_PATTERN = r'^([А-Яа-я\s\-]+)(?:\s+под\s+([А-Яа-я\s]+))?'
DEPARTMENT_PATTERN = r'Отд\s+(\d+)\s+(\d+)/(\d+)'
PRODUCTION_UNIT_PATTERN = r'По\s+([А-Яа-я]+)\s+(\d+)/(\d+)'
METRICS_PATTERN = r'(\d+)/(\d+)'

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
    
    Args:
        input_data (Dict[str, List[Dict[str, Any]]]): Входные данные
        
    Returns:
        List[Dict[str, Any]]: Список распарсенных сообщений
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
    
    Args:
        payload (str): Текст сообщения
        date (Optional[str]): Дата сообщения
        
    Returns:
        List[Dict[str, Any]]: Список распарсенных операций
    """
    try:
        # Извлечение даты если не предоставлена
        if not date:
            date = extract_date_from_payload(payload)
            
        # Разделение на блоки операций
        operation_blocks = split_into_operation_blocks(payload)
        parsed_operations = []
        
        # Парсинг каждого блока
        for block in operation_blocks:
            operations = parse_operation_block(block, date)
            parsed_operations.extend(operations)
            
        return parsed_operations
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге payload: {str(e)}")
        raise

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

def extract_date_from_payload(payload: str) -> Optional[str]:
    """
    Извлечение даты из текста.
    
    Args:
        payload (str): Текст сообщения
        
    Returns:
        Optional[str]: Дата в формате DD.MM или None
    """
    match = re.search(DATE_PATTERN, payload)
    if match:
        day, month, year = match.groups()
        if year:
            return f"{day}.{month}.{year}"
        return f"{day}.{month}"
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
        crop = correct_crop(crop) if crop else None
        
        # Поиск данных по отделам
        department_matches = re.finditer(DEPARTMENT_PATTERN, block)
        operations = []
        
        for match in department_matches:
            dept_num, daily_area, total_area = map(int, match.groups())
            
            operation_data = {
                'date': date,
                'operation': operation,
                'crop': crop,
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
                'crop': crop,
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
        raise

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