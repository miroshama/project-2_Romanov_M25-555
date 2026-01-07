import json
from typing import Any, Dict

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