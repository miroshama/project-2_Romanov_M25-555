from typing import Any, Dict, List, Optional

ALLOWED_TYPES = {'int', 'str', 'bool'}

def create_table(metadata: Dict[str, Any], table_name: str, columns: List[str]) -> Dict[str, Any]:
    '''
    Функция создания таблицы
    
    Переменная metadata: Переданные данные
    Переменная table_name: Название таблицы
    Переменная columns: Название столбца
    '''
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    user_col_names = [col.split(':')[0].lower() for col in columns if ':' in col]

    table_schema = []
    
    if 'id' not in user_col_names:
        table_schema.append({'name': 'ID', 'type': 'int'})

    for col_def in columns:
        if ':' not in col_def:
            print(f"Ошибка: Неверный формат столбца '{col_def}'.")
            return metadata
        
        col_name, col_type = col_def.split(':', 1)
        
        if col_type not in ALLOWED_TYPES:
            print(f"Ошибка: Недопустимый тип данных '{col_type}'.")
            return metadata
        
        final_name = 'ID' if col_name.lower() == 'id' else col_name
        table_schema.append({'name': final_name, 'type': col_type})
        

    metadata[table_name] = table_schema
    
    col_str_list = [f"{col['name']}:{col['type']}" for col in table_schema]
    col_output = ", ".join(col_str_list)
    
    print(f'Таблица "{table_name}" успешно создана со столбцами: {col_output}')
    return metadata

def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    '''
    Функция для удаления таблицы
    
    Перемнная metadata: Переданные данные
    Перемнная table_name: Название таблицы
    '''
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

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