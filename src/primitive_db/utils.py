import json
import os
from typing import Any, Dict, List

DATA_DIR = 'data'

def load_metadata(filepath: str) -> Dict[str, Any]:
    '''
    Функция для загрузки данных из json
    
    Переменная filepath: путь до json файла
    '''
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    '''
    Docstring for save_metadata
    
    Переменная filepath: путь до json файла
    Переменная data: переданные данные
    '''
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_table_data(table_name: str) -> List[Dict[str, Any]]:
    '''
    Функция для загрузки данных таблиц
    
    Перемнная table_name: Название таблицы
    '''
    filepath = os.path.join(DATA_DIR, f"{table_name}.json")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_table_data(table_name: str, data: List[Dict[str, Any]]) -> None:
    '''
    Функция для сохранения записей таблиц
    
    Перемнная table_name: Название таблицы
    Переменная data: Данные 
    '''
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    filepath = os.path.join(DATA_DIR, f"{table_name}.json")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

        