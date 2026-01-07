# src/primitive_db/constants.py

import os

# абсолютный путь
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

# путь до /data
DATA_DIR = os.path.join(BASE_DIR, 'data') 

# путь до файла с метаданными
DB_FILE_NAME = 'db_meta.json' 

# Полный путь к файлу метаданных 
DB_META_PATH = os.path.join(BASE_DIR, DB_FILE_NAME) 

# Допустимые типы данных для колонок
ALLOWED_TYPES = {'int', 'str', 'bool'}
