from typing import Any, Dict, List

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
        
        table_schema.append({'name': col_name, 'type': col_type})

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