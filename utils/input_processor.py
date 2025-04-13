#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для обработки входных JSON данных.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Union, Any

logger = logging.getLogger(__name__)

def load_input_json(file_path: Union[str, Path]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Загрузка и валидация входного JSON файла.
    
    Args:
        file_path (Union[str, Path]): Путь к JSON файлу
        
    Returns:
        Dict[str, List[Dict[str, Any]]]: Загруженные и валидированные данные
        
    Raises:
        FileNotFoundError: Если файл не существует
        json.JSONDecodeError: Если файл содержит некорректный JSON
        ValueError: Если структура данных не соответствует ожидаемой
    """
    try:
        # Преобразование пути в Path объект
        file_path = Path(file_path)
        
        # Проверка существования файла
        if not file_path.exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")
            
        # Чтение и парсинг JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Проверка наличия ключа 'messages' или 'reports'
        if 'messages' not in data and 'reports' not in data:
            raise ValueError("Входной файл должен содержать ключ 'messages' или 'reports'")
            
        # Получение списка сообщений
        messages = data.get('messages', data.get('reports', []))
        
        # Проверка типа данных
        if not isinstance(messages, list):
            raise ValueError("Поле 'messages' или 'reports' должно быть списком")
            
        # Валидация каждого сообщения
        for i, message in enumerate(messages, 1):
            if not isinstance(message, dict):
                raise ValueError(f"Сообщение {i} должно быть словарем")
                
            if 'payload' not in message:
                raise ValueError(f"Сообщение {i} не содержит обязательного поля 'payload'")
                
            # Добавление message_number если есть id
            if 'id' in message and 'message_number' not in message:
                message['message_number'] = message['id']
                
        logger.info(f"Успешно загружен файл {file_path}")
        return data
        
    except FileNotFoundError as e:
        logger.error(f"Ошибка при загрузке файла: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка при парсинге JSON: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Ошибка валидации данных: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке файла: {str(e)}")
        raise

def prepare_messages(input_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Подготовка и нормализация сообщений.
    
    Args:
        input_data (Dict[str, List[Dict[str, Any]]]): Входные данные
        
    Returns:
        List[Dict[str, Any]]: Подготовленные сообщения
        
    Raises:
        ValueError: Если данные не могут быть нормализованы
    """
    try:
        # Получение списка сообщений
        messages = input_data.get('messages', input_data.get('reports', []))
        prepared_messages = []
        
        for message in messages:
            # Создание базовой структуры сообщения
            prepared_message = {
                'message_number': message.get('message_number', message.get('id')),
                'date': message.get('date', ''),
                'payload': message.get('payload', ''),
                'parsed': []  # Для хранения результатов парсинга
            }
            
            # Добавление дополнительных полей, если они существуют
            for key in ['id', 'source', 'timestamp']:
                if key in message:
                    prepared_message[key] = message[key]
                    
            prepared_messages.append(prepared_message)
            
        logger.info(f"Подготовлено {len(prepared_messages)} сообщений")
        return prepared_messages
        
    except Exception as e:
        logger.error(f"Ошибка при подготовке сообщений: {str(e)}")
        raise ValueError(f"Ошибка при подготовке сообщений: {str(e)}")

def validate_message_structure(message: Dict[str, Any]) -> bool:
    """
    Валидация структуры отдельного сообщения.
    
    Args:
        message (Dict[str, Any]): Сообщение для валидации
        
    Returns:
        bool: True если структура валидна, иначе False
    """
    required_fields = ['message_number', 'payload']
    
    for field in required_fields:
        if field not in message:
            logger.warning(f"Сообщение не содержит обязательного поля '{field}'")
            return False
            
    if not isinstance(message['payload'], str):
        logger.warning("Поле 'payload' должно быть строкой")
        return False
        
    return True 