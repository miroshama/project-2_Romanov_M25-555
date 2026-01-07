# src/primitive_db/engine.py

import shlex

import prompt
from prettytable import PrettyTable

from src.primitive_db import core, parser, utils
from src.primitive_db.constants import DB_META_PATH


def print_help():
    '''
    Функция вывода справочной информации
    '''
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print(
        "<command> create_table <имя_таблицы> <столбец1:тип> .. "
        "- создать таблицу"
    )
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\n***Операции с данными***")
    print(
        "<command> insert into <имя_таблицы> values (<значение1>, ..) "
        "- создать запись"
    )
    print(
        "<command> select from <имя_таблицы> [where <col> = <val>] "
        "- читать записи"
    )
    print(
        "<command> update <имя_таблицы> set <col> = <val> "
        "where <col> = <val> - обновить"
    )
    print(
        "<command> delete from <имя_таблицы> where <col> = <val> "
        "- удалить запись"
    )
    print("<command> info <имя_таблицы> - информация о таблице")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def run():
    '''
    Главная функция
    '''
    print_help()

    while True:
        metadata = utils.load_metadata(DB_META_PATH)
        
        try:
            raw_input = prompt.string('>>>Введите команду: ')
            if not raw_input or not raw_input.strip():
                continue
            
            args = shlex.split(raw_input)
            command = args[0].lower()

            if command == 'exit':
                break
            
            elif command == 'help':
                print_help()
            
            elif command == 'create_table':
                if len(args) < 2:
                    print("Ошибка: Укажите имя таблицы.")
                else:
                    table_name = args[1]
                    columns = args[2:]
                    new_metadata = core.create_table(metadata, table_name, columns)
                    if new_metadata is not None:
                        utils.save_metadata(DB_META_PATH, new_metadata)

            elif command == 'drop_table':
                if len(args) < 2:
                    print("Ошибка: Укажите имя таблицы.")
                else:
                    table_name = args[1]
                    new_metadata = core.drop_table(metadata, table_name)
                    if new_metadata is not None:
                        utils.save_metadata(DB_META_PATH, new_metadata)
            
            elif command == 'list_tables':
                core.list_tables(metadata)
                
            elif command == 'insert':
                if len(args) < 3:
                    print("Ошибка: Неверный формат команды insert.")
                    continue
                    
                table_name = args[2]
                vals = parser.parse_insert_values(raw_input)
                
                table_data = utils.load_table_data(table_name)
                
                result = core.insert(metadata, table_name, vals)
                
                if not isinstance(result, tuple):
                    continue
                
                new_row, _ = result
                
                if table_data:
                    new_id = max(row['ID'] for row in table_data) + 1
                else:
                    new_id = 1
                
                new_row['ID'] = new_id
                
                table_data.append(new_row)
                utils.save_table_data(table_name, table_data)
                print(
                    f'Запись с ID={new_id} успешно добавлена '
                    f'в таблицу "{table_name}".'
                )

            elif command == 'select':
                if len(args) < 3:
                    print("Ошибка: Укажите таблицу (select from <table>).")
                    continue
                
                table_name = args[2]
                table_data = utils.load_table_data(table_name)
                where_clause = parser.parse_where(args)
                
                results = core.select(table_data, where_clause)
                
                if results is None:
                    continue

                if table_name in metadata:
                    pt = PrettyTable()
                    field_names = [col['name'] for col in metadata[table_name]]
                    pt.field_names = field_names
                    
                    for row in results:
                        pt.add_row([row.get(name) for name in field_names])
                    print(pt)
                else:
                    print(f"Ошибка: Метаданные для таблицы {table_name} не найдены.")

            elif command == 'update':
                if len(args) < 6:
                    print("Ошибка: Неверный формат update.")
                    continue

                table_name = args[1]
                table_data = utils.load_table_data(table_name)
                
                set_col = args[3]
                raw_val = args[5]
                
                target_type = 'str' 
                if table_name in metadata:
                    for col in metadata[table_name]:
                        if col['name'] == set_col:
                            target_type = col['type']
                            break
                
                set_val = core.cast_value(raw_val, target_type)
                where_clause = parser.parse_where(args)
                
                result = core.update(table_data, {set_col: set_val}, where_clause)
                
                if not isinstance(result, tuple):
                    continue

                new_data, updated_ids = result
                utils.save_table_data(table_name, new_data)
                
                if updated_ids:
                    print(
                        f'Запись с ID={updated_ids[0]} в таблице '
                        f'"{table_name}" успешно обновлена.'
                    )
                else:
                    print("Ни одной записи не было обновлено.")

            elif command == 'delete':
                if len(args) < 3:
                    print("Ошибка: Укажите таблицу.")
                    continue

                table_name = args[2]
                table_data = utils.load_table_data(table_name)
                where_clause = parser.parse_where(args)
                
                result = core.delete(table_data, where_clause)

                if not isinstance(result, tuple):
                    continue

                new_data, deleted_ids = result
                utils.save_table_data(table_name, new_data)
                
                if deleted_ids:
                    print(
                        f'Запись с ID={deleted_ids[0]} успешно удалена '
                        f'из таблицы "{table_name}".'
                    )
                else:
                    print("Записи для удаления не найдены.")

            elif command == 'info':
                if len(args) < 2:
                    print("Ошибка: Укажите имя таблицы.")
                    continue
                
                table_name = args[1]
                if table_name in metadata:
                    table_data = utils.load_table_data(table_name)
                    schema = metadata[table_name]
                    
                    col_str_list = [f"{col['name']}:{col['type']}" for col in schema]
                    col_output = ", ".join(col_str_list)
                    
                    print(f"Таблица: {table_name}")
                    print(f"Столбцы: {col_output}")
                    print(f"Количество записей: {len(table_data)}")
                else:
                    print(f"Таблица {table_name} не найдена.")

            else:
                print(f"Неизвестная команда: {command}")
            
            print() 

        except Exception as e:
            print(f"Произошла ошибка: {e}")