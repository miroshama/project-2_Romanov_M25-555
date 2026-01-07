# src/primitive_db/decorators.py

import functools
import time

import prompt


def handle_db_errors(func):
    '''
    Декоратор для перехвата ошибок БД (KeyError, ValueError, FileNotFoundError).
    В случае ошибки выводит сообщение и возвращает первый аргумент
    (обычно metadata или data).
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, таблица еще не создана.")
            return args[0] if args else None
        except KeyError as e:
            print(f"Ошибка: Некорректный ключ или таблица не найдена: {e}")
            return args[0] if args else None
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
            return args[0] if args else None
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
            return args[0] if args else None
    return wrapper

def confirm_action(action_name: str):
    '''
    Фабрика декораторов. Запрашивает подтверждение перед выполнением функции
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Разбиваем длинную f-строку на две части
            msg = (
                f'Вы уверены, что хотите выполнить "{action_name}"? '
                f'[y/n]: '
            )
            answer = prompt.string(msg)
            
            if answer.lower() == 'y':
                return func(*args, **kwargs)
            else:
                print("Операция отменена.")
                return args[0] if args else None
        return wrapper
    return decorator

def log_time(func):
    '''
    Декоратор для замера времени выполнения функции
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        duration = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {duration:.4f} секунд.")
        return result
    return wrapper