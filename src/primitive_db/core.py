# src/primitive_db/core.py

from typing import Any, Dict, List, Optional, Tuple

# Импортируем типы и декораторы
from src.primitive_db.constants import ALLOWED_TYPES
from src.primitive_db.decorators import confirm_action, handle_db_errors, log_time

def create_cacher():
    """Создает кэшер с замкнутым словарем cache."""
    cache = {}

    def cache_result(key: str, value_func):
        if key in cache:
            return cache[key]
        
        result = value_func()
        cache[key] = result
        return result

    return cache_result

select_cacher = create_cacher()

def cast_value(value: str, target_type: str) -> Any:
    '''
    функция для приведения значения к нужному типу
    
    Переменная value: Изначальное значение
    Переменная target_type: Значение в нужном виде
    '''
    if target_type == 'int':
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"Невозможно преобразовать '{value}' в int")
    if target_type == 'bool':
        lower_val = value.lower()
        if lower_val in ('true', '1', 'yes'):
            return True
        if lower_val in ('false', '0', 'no'):
            return False
        raise ValueError(f"Невозможно преобразовать '{value}' в bool")
    return str(value).strip('"').strip("'")

@handle_db_errors
def create_table(metadata: Dict[str, Any], table_name: str, columns: List[str]) -> Dict[str, Any]:
    '''
    Функция создания таблицы
    
    Переменная metadata: Переданные данные
    Переменная table_name: Название таблицы
    Переменная columns: Название столбца
    '''
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')

    user_col_names = [col.split(':')[0].lower() for col in columns if ':' in col]
    table_schema = []
    
    if 'id' not in user_col_names:
        table_schema.append({'name': 'ID', 'type': 'int'})

    for col_def in columns:
        if ':' not in col_def:
            raise ValueError(f"Неверный формат столбца '{col_def}'. Используйте имя:тип")
        
        col_name, col_type = col_def.split(':', 1)
        
        if col_type not in ALLOWED_TYPES:
            raise ValueError(f"Недопустимый тип данных '{col_type}'. Доступны: {ALLOWED_TYPES}")
        
        final_name = 'ID' if col_name.lower() == 'id' else col_name
        table_schema.append({'name': final_name, 'type': col_type})

    metadata[table_name] = table_schema
    
    col_str_list = [f"{col['name']}:{col['type']}" for col in table_schema]
    print(f'Таблица "{table_name}" успешно создана со столбцами: {", ".join(col_str_list)}')
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    '''
    Функция для удаления таблицы
    
    Перемнная metadata: Переданные данные
    Перемнная table_name: Название таблицы
    '''
    if table_name not in metadata:
        raise KeyError(f'Таблица "{table_name}" не существует.')

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata


def list_tables(metadata: Dict[str, Any]) -> None:
    '''
    Функция для списка таблиц
    
    Перемнная metadata: переданные данные
    '''
    for name in metadata:
        print(f"- {name}")


@handle_db_errors
@log_time
def insert(metadata: Dict[str, Any], table_name: str, values: List[str]) -> Tuple[Dict[str, Any], int]:
    if table_name not in metadata:
        raise KeyError(f'Таблица "{table_name}" не найдена в метаданных.')
        
    schema = metadata[table_name] 
    
    if len(values) != len(schema) - 1:
        raise ValueError(f"Ожидалось {len(schema)-1} значений, получено {len(values)}")

    new_row = {}
    for i, col_info in enumerate(schema[1:]):
        val = cast_value(values[i], col_info['type'])
        new_row[col_info['name']] = val
    
    return new_row, 0


@handle_db_errors
@log_time
def select(table_data: List[Dict[str, Any]], where_clause: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    cache_key = str(where_clause) if where_clause else "ALL"

    def perform_select():
        if not where_clause:
            return table_data
        
        result = []
        for row in table_data:
            match = True
            for key, val in where_clause.items():
                if str(row.get(key)) != str(val):
                    match = False
                    break
            if match:
                result.append(row)
        return result

    return select_cacher(cache_key, perform_select)


@handle_db_errors
def update(table_data: List[Dict[str, Any]], set_clause: Dict[str, Any], where_clause: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[int]]:
    if not where_clause:
        raise ValueError("Для обновления необходимо условие where")

    updated_ids = []
    found_match = False
    
    for row in table_data:
        match = True
        for key, val in where_clause.items():
            if str(row.get(key)) != str(val):
                match = False
                break
        if match:
            found_match = True
            row.update(set_clause)
            updated_ids.append(row['ID'])
    
    if not found_match:
        pass 
        
    return table_data, updated_ids


@handle_db_errors
@confirm_action("удаление записи")
def delete(table_data: List[Dict[str, Any]], where_clause: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[int]]:
    if not where_clause:
        raise ValueError("Для удаления необходимо условие where")

    new_data = []
    deleted_ids = []
    
    for row in table_data:
        match = True
        for key, val in where_clause.items():
            if str(row.get(key)) != str(val):
                match = False
                break
        if match:
            deleted_ids.append(row['ID'])
        else:
            new_data.append(row)
            
    return new_data, deleted_ids



def cast_value(value: str, target_type: str) -> Any:
    '''
    функция для приведения значения к нужному типу
    
    Переменная value: Изначальное значение
    Переменная target_type: Значение в нужном виде
    '''
    if target_type == 'int':
        return int(value)
    if target_type == 'bool':
        return value.lower() in ('true', '1', 'yes')
    return str(value).strip('"').strip("'")

def insert(metadata: Dict[str, Any], table_name: str, values: List[str]) -> tuple[List[Dict[str, Any]], int]:
    '''
    Функция реализации insert
    
    Переменная metadata: Вводимые данные
    Переменная table_name: Название таблицы
    Переменная values: Значение
    '''
    schema = metadata[table_name] 
    
    if len(values) != len(schema) - 1:
        raise ValueError(f"Ожидалось {len(schema)-1} значений, получено {len(values)}")

    new_row = {}

    for i, col_info in enumerate(schema[1:]):
        new_row[col_info['name']] = cast_value(values[i], col_info['type'])
    
    return new_row, 0

def select(table_data: List[Dict[str, Any]], where_clause: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    '''
    Функция реализации select
    
    Переменная table_data: Название таблицы
    Переменная where_clause: условие для where
    '''
    if not where_clause:
        return table_data
    
    result = []
    for row in table_data:
        match = True
        for key, val in where_clause.items():
            if str(row.get(key)) != str(val):
                match = False
                break
        if match:
            result.append(row)
    return result

def update(table_data: List[Dict[str, Any]], set_clause: Dict[str, Any], where_clause: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[int]]:
    '''
    Функция для реализации update
    
    Переменная table_data: Данные таблицы
    Переменная set_clause: Новое значение
    Переменная where_clause: Значение условия
    '''
    updated_ids = []
    for row in table_data:
        match = True
        for key, val in where_clause.items():
            if str(row.get(key)) != str(val):
                match = False
                break
        if match:
            row.update(set_clause)
            updated_ids.append(row['ID'])
    return table_data, updated_ids

def delete(table_data: List[Dict[str, Any]], where_clause: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[int]]:
    '''
    Функция для реализации delete
    
    Переменная table_data: значение в таблице
    Переменная where_clause: значение условия
    '''
    new_data = []
    deleted_ids = []
    for row in table_data:
        match = True
        for key, val in where_clause.items():
            if str(row.get(key)) != str(val):
                match = False
                break
        if match:
            deleted_ids.append(row['ID'])
        else:
            new_data.append(row)
    return new_data, deleted_ids