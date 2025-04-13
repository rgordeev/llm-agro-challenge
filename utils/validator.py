#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для валидации распарсенных данных.
"""

import logging
from typing import List, Dict, Any, Optional, Union
from difflib import get_close_matches

from config.reference_data import (
    VALID_OPERATIONS,
    VALID_CROPS,
    DIVISIONS,
    correct_operation,
    correct_crop
)

logger = logging.getLogger(__name__)

def validate_parsed_data(parsed_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Валидация всех распарсенных сообщений.
    
    Args:
        parsed_messages (List[Dict[str, Any]]): Список распарсенных сообщений
        
    Returns:
        List[Dict[str, Any]]: Список валидированных сообщений
    """
    validated_messages = []
    
    for message in parsed_messages:
        try:
            # Валидация каждого сообщения
            validated_message = {
                'message_number': message.get('message_number'),
                'date': message.get('date'),
                'payload': message.get('payload'),
                'parsed': []
            }
            
            # Валидация операций
            for operation in message.get('parsed', []):
                validated_operation = validate_operation(operation)
                if validated_operation:
                    validated_message['parsed'].append(validated_operation)
                    
            validated_messages.append(validated_message)
            
        except Exception as e:
            logger.error(f"Ошибка при валидации сообщения: {str(e)}")
            continue
            
    return validated_messages

def validate_operation(operation: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Валидация отдельной операции.
    
    Args:
        operation (Dict[str, Any]): Операция для валидации
        
    Returns:
        Optional[Dict[str, Any]]: Валидированная операция или None
    """
    try:
        # Валидация типа операции
        operation_type = validate_operation_type(operation.get('operation'))
        if not operation_type:
            logger.warning(f"Невалидный тип операции: {operation.get('operation')}")
            return None
            
        # Валидация культуры
        crop = validate_crop(operation.get('crop'))
        
        # Валидация подразделения
        division = validate_division(operation.get('division'))
        
        # Валидация числовых значений
        validated_operation = validate_numeric_values(operation)
        
        # Обновление валидированных полей
        validated_operation.update({
            'operation': operation_type,
            'crop': crop,
            'division': division
        })
        
        return validated_operation
        
    except Exception as e:
        logger.error(f"Ошибка при валидации операции: {str(e)}")
        return None

def validate_operation_type(operation_type: Optional[str]) -> Optional[str]:
    """
    Валидация типа операции.
    
    Args:
        operation_type (Optional[str]): Тип операции
        
    Returns:
        Optional[str]: Валидный тип операции или None
    """
    if not operation_type:
        return None
        
    # Точное совпадение
    if operation_type in VALID_OPERATIONS:
        return operation_type
        
    # Регистронезависимое совпадение
    operation_lower = operation_type.lower()
    for valid_op in VALID_OPERATIONS:
        if valid_op.lower() == operation_lower:
            return valid_op
            
    # Нечеткое совпадение
    matches = get_close_matches(operation_type, VALID_OPERATIONS, n=1, cutoff=0.8)
    if matches:
        logger.info(f"Исправлен тип операции: {operation_type} -> {matches[0]}")
        return matches[0]
        
    # Попытка исправления через словарь исправлений
    corrected = correct_operation(operation_type)
    if corrected in VALID_OPERATIONS:
        logger.info(f"Исправлен тип операции через словарь: {operation_type} -> {corrected}")
        return corrected
        
    return None

def validate_crop(crop: Optional[str]) -> Optional[str]:
    """
    Валидация культуры.
    
    Args:
        crop (Optional[str]): Название культуры
        
    Returns:
        Optional[str]: Валидное название культуры или None
    """
    if not crop:
        return None
        
    # Точное совпадение
    if crop in VALID_CROPS:
        return crop
        
    # Регистронезависимое совпадение
    crop_lower = crop.lower()
    for valid_crop in VALID_CROPS:
        if valid_crop.lower() == crop_lower:
            return valid_crop
            
    # Нечеткое совпадение
    matches = get_close_matches(crop, VALID_CROPS, n=1, cutoff=0.8)
    if matches:
        logger.info(f"Исправлена культура: {crop} -> {matches[0]}")
        return matches[0]
        
    # Попытка исправления через словарь исправлений
    corrected = correct_crop(crop)
    if corrected in VALID_CROPS:
        logger.info(f"Исправлена культура через словарь: {crop} -> {corrected}")
        return corrected
        
    # Обработка общих паттернов
    if "пшеница" in crop_lower and "озим" in crop_lower:
        return "Озимая пшеница"
    if "ячмень" in crop_lower and "озим" in crop_lower:
        return "Озимый ячмень"
    if "ячмень" in crop_lower and "яров" in crop_lower:
        return "Яровой ячмень"
        
    return None

def validate_division(division: Optional[str]) -> str:
    """
    Валидация подразделения.
    
    Args:
        division (Optional[str]): Название подразделения
        
    Returns:
        str: Валидное название подразделения
    """
    if not division:
        return "АОР"
        
    # Точное совпадение
    if division in DIVISIONS:
        return division
        
    # Регистронезависимое совпадение
    division_lower = division.lower()
    for valid_div in DIVISIONS:
        if valid_div.lower() == division_lower:
            return valid_div
            
    # Нечеткое совпадение
    matches = get_close_matches(division, list(DIVISIONS.keys()), n=1, cutoff=0.8)
    if matches:
        logger.info(f"Исправлено подразделение: {division} -> {matches[0]}")
        return matches[0]
        
    return "АОР"

def validate_numeric_values(operation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валидация числовых значений.
    
    Args:
        operation (Dict[str, Any]): Операция с числовыми значениями
        
    Returns:
        Dict[str, Any]: Операция с валидированными числовыми значениями
    """
    validated = operation.copy()
    
    # Поля для валидации
    numeric_fields = ['dailyArea', 'totalArea', 'dailyYield', 'totalYield']
    
    for field in numeric_fields:
        value = operation.get(field)
        if value is None:
            continue
            
        try:
            # Преобразование в число
            if isinstance(value, str):
                # Удаление пробелов и других символов
                value = ''.join(c for c in value if c.isdigit() or c == '.')
                if not value:
                    validated[field] = None
                    continue
                    
            # Преобразование в float или int
            if '.' in str(value):
                num_value = float(value)
            else:
                num_value = int(value)
                
            # Проверка на разумные значения
            if field in ['dailyArea', 'totalArea']:
                if num_value < 0 or num_value > 10000:  # Максимальная площадь 10000 га
                    logger.warning(f"Нереалистичное значение {field}: {num_value}")
                    validated[field] = None
                else:
                    validated[field] = num_value
                    
            elif field in ['dailyYield', 'totalYield']:
                if num_value < 0 or num_value > 100:  # Максимальная урожайность 100 т/га
                    logger.warning(f"Нереалистичное значение {field}: {num_value}")
                    validated[field] = None
                else:
                    validated[field] = num_value
                    
        except (ValueError, TypeError):
            logger.warning(f"Некорректное значение {field}: {value}")
            validated[field] = None
            
    return validated 