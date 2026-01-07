# src/primitive_db/engine.py

import prompt
import shlex
from src.primitive_db.utils import load_metadata, save_metadata
from src.primitive_db import core

DB_FILE = 'db_meta.json'

def print_help():
    '''
    Функция вывода справочной информации
    '''
   
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    
    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def run():
    '''
    Функция главной функции
    '''
    print_help()

    while True:
        metadata = load_metadata(DB_FILE)
        
        try:
            user_input = prompt.string('>>>Введите команду: ')
            if not user_input or not user_input.strip():
                continue
            
            args = shlex.split(user_input)
            command = args[0]

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
                    save_metadata(DB_FILE, new_metadata)

            elif command == 'drop_table':
                if len(args) < 2:
                    print("Ошибка: Укажите имя таблицы.")
                else:
                    table_name = args[1]
                    new_metadata = core.drop_table(metadata, table_name)
                    save_metadata(DB_FILE, new_metadata)
            
            elif command == 'list_tables':
                core.list_tables(metadata)
            
            else:
                print(f"Неизвестная команда: {command}")
            
            print() 

        except Exception as e:
            print(f"Произошла ошибка: {e}")