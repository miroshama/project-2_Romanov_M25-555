from typing import Any, Dict, List, Optional, Tuple

from src.primitive_db.constants import ALLOWED_TYPES
from src.primitive_db.decorators import confirm_action, handle_db_errors, log_time


def create_cacher():
    '''
    Функция для создания кэшера
    '''
    cache = {}

    def cache_result(key: str = None, value_func=None, clear: bool = False):
        if clear:
            cache.clear()
            return None
        if key in cache:
            return cache[key]
        if value_func:
            result = value_func()
            cache[key] = result
            return result
        return None

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
def create_table(
    metadata: Dict[str, Any], table_name: str, columns: List[str]
) -> Dict[str, Any]:
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
            raise ValueError(
                f"Неверный формат столбца '{col_def}'. "
                f"Используйте имя:тип"
            )

        col_name, col_type = col_def.split(':', 1)

        if col_type not in ALLOWED_TYPES:
            raise ValueError(
                f"Недопустимый тип данных '{col_type}'. "
                f"Доступны: {ALLOWED_TYPES}"
            )

        final_name = 'ID' if col_name.lower() == 'id' else col_name
        table_schema.append({'name': final_name, 'type': col_type})

    metadata[table_name] = table_schema

    col_str_list = [f"{col['name']}:{col['type']}" for col in table_schema]
    print(
        f'Таблица "{table_name}" успешно создана '
        f'со столбцами: {", ".join(col_str_list)}'
    )
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
    select_cacher(clear=True)
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
def insert(
    metadata: Dict[str, Any], table_name: str, values: List[str]
) -> Tuple[Dict[str, Any], int]:
    '''
    Функция реализации insert
    
    Переменная metadata: Вводимые данные
    Переменная table_name: Название таблицы
    Переменная values: Значение
    '''
    if table_name not in metadata:
        raise KeyError(f'Таблица "{table_name}" не найдена в метаданных.')

    schema = metadata[table_name]

    if len(values) != len(schema) - 1:
        raise ValueError(
            f"Ожидалось {len(schema)-1} значений, получено {len(values)}"
        )

    select_cacher(clear=True)

    new_row = {}
    for i, col_info in enumerate(schema[1:]):
        val = cast_value(values[i], col_info['type'])
        new_row[col_info['name']] = val

    return new_row, 0


@handle_db_errors
@log_time
def select(
    table_data: List[Dict[str, Any]],
    where_clause: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    '''
    Функция реализации select
    
    Переменная table_data: Название таблицы
    Переменная where_clause: условие для where
    '''
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

    return select_cacher(key=cache_key, value_func=perform_select)

@handle_db_errors
def update(
    table_data: List[Dict[str, Any]],
    set_clause: Dict[str, Any],
    where_clause: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[int]]:
    '''
    Функция для реализации update
    
    Переменная table_data: Данные таблицы
    Переменная set_clause: Новое значение
    Переменная where_clause: Значение условия
    '''
    if not where_clause:
        raise ValueError("Для обновления необходимо условие where")

    # Очищаем кэш при обновлении
    select_cacher(clear=True)

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

@handle_db_errors
@confirm_action("удаление записи")
def delete(
    table_data: List[Dict[str, Any]],
    where_clause: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[int]]:
    '''
    Функция для реализации delete
    
    Переменная table_data: значение в таблице
    Переменная where_clause: значение условия
    '''
    if not where_clause:
        raise ValueError("Для удаления необходимо условие where")

    select_cacher(clear=True)

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