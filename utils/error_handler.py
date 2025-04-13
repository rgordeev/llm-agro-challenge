#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для обработки ошибок в валидированных данных.
"""

import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Словарь совместимости операций и культур
OPERATION_CROP_COMPATIBILITY = {
    "Пахота": ["Многолетние травы", "Озимая пшеница", "Озимый ячмень", "Яровой ячмень"],
    "Сев": ["Озимая пшеница", "Озимый ячмень", "Яровой ячмень", "Горох", "Подсолнечник"],
    "Уборка": ["Озимая пшеница", "Озимый ячмень", "Яровой ячмень", "Горох", "Подсолнечник"],
    "Боронование": ["Озимая пшеница", "Озимый ячмень", "Яровой ячмень", "Горох"],
    "Культивация": ["Подсолнечник", "Кукуруза", "Соя"],
    "Гербицидная обработка": ["Озимая пшеница", "Озимый ячмень", "Яровой ячмень", "Подсолнечник"],
    "Внесение минеральных удобрений": ["Озимая пшеница", "Озимый ячмень", "Яровой ячмень", "Горох"]
}

def handle_errors(validated_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Обработка ошибок в валидированных данных.
    
    Args:
        validated_data (List[Dict[str, Any]]): Валидированные данные
        
    Returns:
        List[Dict[str, Any]]: Исправленные данные
    """
    corrected_data = []
    
    for message in validated_data:
        try:
            # Получение операций
            operations = message.get('parsed', [])
            if not operations:
                continue
                
            # Проверка отсутствующих полей
            operations = check_missing_fields(operations, message.get('payload', ''))
            
            # Применение согласованности дат
            operations = apply_date_consistency(operations)
            
            # Корректировка каждой операции
            corrected_operations = []
            for operation in operations:
                corrected_operation = correct_operation(operation)
                if corrected_operation:
                    corrected_operations.append(corrected_operation)
                    
            # Обновление сообщения
            corrected_message = message.copy()
            corrected_message['parsed'] = corrected_operations
            corrected_data.append(corrected_message)
            
        except Exception as e:
            logger.error(f"Ошибка при обработке сообщения: {str(e)}")
            continue
            
    return corrected_data

def correct_operation(operation: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Корректировка отдельной операции.
    
    Args:
        operation (Dict[str, Any]): Операция для корректировки
        
    Returns:
        Optional[Dict[str, Any]]: Исправленная операция или None
    """
    try:
        corrected = operation.copy()
        
        # Корректировка формата даты
        if 'date' in corrected:
            corrected['date'] = correct_date_format(corrected['date'])
            
        # Проверка совместимости культуры и операции
        corrected = check_crop_operation_consistency(corrected)
        
        # Проверка числовой согласованности
        corrected = check_numeric_consistency(corrected)
        
        return corrected
        
    except Exception as e:
        logger.error(f"Ошибка при корректировке операции: {str(e)}")
        return None

def correct_date_format(date_str: Optional[str]) -> Optional[str]:
    """
    Корректировка формата даты.
    
    Args:
        date_str (Optional[str]): Дата для корректировки
        
    Returns:
        Optional[str]: Исправленная дата или None
    """
    if not date_str:
        return None
        
    try:
        # Разбор различных форматов даты
        patterns = [
            r'(\d{1,2})[\./](\d{1,2})[\./](\d{4})',  # DD.MM.YYYY
            r'(\d{1,2})[\./](\d{1,2})[\./](\d{2})',  # DD.MM.YY
            r'(\d{1,2})[\./](\d{1,2})'                # DD.MM
        ]
        
        for pattern in patterns:
            match = re.match(pattern, date_str)
            if match:
                day, month, *year = match.groups()
                day = day.zfill(2)
                month = month.zfill(2)
                
                if year:
                    year = year[0]
                    if len(year) == 2:
                        year = f"20{year}"  # Предполагаем 21 век
                    return f"{day}.{month}.{year}"
                else:
                    return f"{day}.{month}"
                    
        logger.warning(f"Не удалось распознать формат даты: {date_str}")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при корректировке даты: {str(e)}")
        return None

def check_crop_operation_consistency(operation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проверка совместимости культуры и операции.
    
    Args:
        operation (Dict[str, Any]): Операция для проверки
        
    Returns:
        Dict[str, Any]: Исправленная операция
    """
    corrected = operation.copy()
    operation_type = corrected.get('operation')
    crop = corrected.get('crop')
    
    if not operation_type or not crop:
        return corrected
        
    # Проверка совместимости
    compatible_crops = OPERATION_CROP_COMPATIBILITY.get(operation_type, [])
    if crop not in compatible_crops:
        logger.warning(f"Несовместимая пара операция-культура: {operation_type} - {crop}")
        
        # Попытка найти совместимую культуру
        for compatible_crop in compatible_crops:
            if crop.lower() in compatible_crop.lower() or compatible_crop.lower() in crop.lower():
                logger.info(f"Исправлена культура: {crop} -> {compatible_crop}")
                corrected['crop'] = compatible_crop
                break
                
    return corrected

def check_numeric_consistency(operation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Проверка числовой согласованности.
    
    Args:
        operation (Dict[str, Any]): Операция для проверки
        
    Returns:
        Dict[str, Any]: Исправленная операция
    """
    corrected = operation.copy()
    
    # Проверка площадей
    daily_area = corrected.get('dailyArea')
    total_area = corrected.get('totalArea')
    
    if daily_area is not None and total_area is not None:
        if daily_area > total_area:
            logger.warning(f"Дневная площадь ({daily_area}) больше общей ({total_area})")
            # Меняем местами значения
            corrected['dailyArea'], corrected['totalArea'] = total_area, daily_area
            
    # Проверка урожайности
    daily_yield = corrected.get('dailyYield')
    total_yield = corrected.get('totalYield')
    
    if daily_yield is not None:
        if daily_yield > 100:  # Нереалистично высокая урожайность
            logger.warning(f"Нереалистично высокая дневная урожайность: {daily_yield}")
            corrected['dailyYield'] = daily_yield / 10  # Уменьшаем в 10 раз
            
    if total_yield is not None:
        if total_yield > 100:  # Нереалистично высокая урожайность
            logger.warning(f"Нереалистично высокая общая урожайность: {total_yield}")
            corrected['totalYield'] = total_yield / 10  # Уменьшаем в 10 раз
            
    return corrected

def check_missing_fields(operations: List[Dict[str, Any]], payload: str) -> List[Dict[str, Any]]:
    """
    Проверка отсутствующих полей.
    
    Args:
        operations (List[Dict[str, Any]]): Список операций
        payload (str): Исходный текст
        
    Returns:
        List[Dict[str, Any]]: Операции с заполненными полями
    """
    if not operations:
        return operations
        
    # Определение общих полей из других операций
    common_fields = {}
    for operation in operations:
        for key, value in operation.items():
            if value is not None:
                common_fields[key] = value
                
    # Заполнение отсутствующих полей
    corrected_operations = []
    for operation in operations:
        corrected = operation.copy()
        
        # Заполнение отсутствующих полей общими значениями
        for key, value in common_fields.items():
            if key not in corrected or corrected[key] is None:
                corrected[key] = value
                logger.info(f"Заполнено отсутствующее поле {key}: {value}")
                
        corrected_operations.append(corrected)
        
    return corrected_operations

def apply_date_consistency(operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Применение согласованности дат.
    
    Args:
        operations (List[Dict[str, Any]]): Список операций
        
    Returns:
        List[Dict[str, Any]]: Операции с согласованными датами
    """
    if not operations:
        return operations
        
    # Поиск первой валидной даты
    base_date = None
    for operation in operations:
        if operation.get('date'):
            base_date = operation['date']
            break
            
    # Применение базовой даты ко всем операциям
    if base_date:
        corrected_operations = []
        for operation in operations:
            corrected = operation.copy()
            if not corrected.get('date'):
                corrected['date'] = base_date
                logger.info(f"Заполнена отсутствующая дата: {base_date}")
            corrected_operations.append(corrected)
            
        return corrected_operations
        
    return operations 