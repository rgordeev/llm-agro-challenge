#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для форматирования исправленных данных в финальную структуру вывода.
"""

import logging
from typing import List, Dict, Any, Union

logger = logging.getLogger(__name__)

def format_output(corrected_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Форматирование всех исправленных данных в финальную структуру вывода.
    
    Args:
        corrected_data (List[Dict[str, Any]]): Исправленные данные
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Отформатированные данные
    """
    try:
        formatted_reports = []
        
        for message in corrected_data:
            formatted_message = format_message(message)
            if formatted_message:
                formatted_reports.append(formatted_message)
                
        return {"reports": formatted_reports}
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании выходных данных: {str(e)}")
        return {"reports": []}

def format_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Форматирование отдельного сообщения.
    
    Args:
        message (Dict[str, Any]): Сообщение для форматирования
        
    Returns:
        Dict[str, Any]: Отформатированное сообщение
    """
    try:
        formatted = {
            "message_number": message.get("message_number", 0),
            "payload": message.get("payload", ""),
            "parsed": []
        }
        
        operations = message.get("parsed", [])
        for operation in operations:
            formatted_operation = format_operation(operation)
            if formatted_operation:
                formatted["parsed"].append(formatted_operation)
                
        return formatted
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании сообщения: {str(e)}")
        return {}

def format_operation(operation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Форматирование отдельной операции.
    
    Args:
        operation (Dict[str, Any]): Операция для форматирования
        
    Returns:
        Dict[str, Any]: Отформатированная операция
    """
    try:
        formatted = {
            "date": operation.get("date"),
            "division": operation.get("division"),
            "operation": operation.get("operation"),
            "crop": operation.get("crop"),
            "dailyArea": format_numeric_value(operation.get("dailyArea")),
            "totalArea": format_numeric_value(operation.get("totalArea")),
            "dailyYield": format_numeric_value(operation.get("dailyYield")),
            "totalYield": format_numeric_value(operation.get("totalYield"))
        }
        
        # Удаляем None значения
        formatted = {k: v for k, v in formatted.items() if v is not None}
        
        return formatted
        
    except Exception as e:
        logger.error(f"Ошибка при форматировании операции: {str(e)}")
        return {}

def format_numeric_value(value: Union[int, float, str, None]) -> Union[int, float, None]:
    """
    Форматирование числового значения.
    
    Args:
        value (Union[int, float, str, None]): Значение для форматирования
        
    Returns:
        Union[int, float, None]: Отформатированное значение
    """
    if value is None:
        return None
        
    try:
        # Преобразование строки в число
        if isinstance(value, str):
            value = value.replace(",", ".")
            if "." in value:
                value = float(value)
            else:
                value = int(value)
                
        # Проверка на целое число
        if isinstance(value, float) and value.is_integer():
            return int(value)
            
        return value
        
    except (ValueError, TypeError):
        logger.warning(f"Невозможно преобразовать значение в число: {value}")
        return None 