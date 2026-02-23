"""
Модуль для загрузки и обработки данных из API Itresume.

Этот модуль выполняет:
1. Скачивание данных из API
2. Валидацию полученных данных
3. Сохранение в базу PostgreSQL

Author: Guzel
Date: 23.02.2026
Version: 1.0
"""

import requests
import ast
from typing import Dict, List, Tuple, Any, Optional
import re
from datetime import datetime, timedelta
import csv
import psycopg2
from psycopg2 import sql
import os
import logging
from pathlib import Path
import time

# Параметры для скачивания данных
API_URL = "https://b2b.itresume.ru/api/statistics"
params = {
    'client' : 'Skillfactory',
    'client_key' : 'M2MGWS',
    'start' : '2023-04-01 12:46:47.860798', # переменная
    'end' : '2023-04-04 12:46:47.860798'    # переменная
}

# Параметры для подключения к базе
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'training_db',
    'user': 'postgres', 
    'password': '123' 
    }

# Параметры для создания папки для хранения файлов по проекту
BASE_PATH = Path.home()
PROJECT_NAME = "training_db"

# ------------------------------------------------------
# Создаем директорию для хранение всех файлов по проекту
# ------------------------------------------------------

def create_project_directories(base_path=None, project_name="my_project"):

    if base_path is None:
        base_path = Path.cwd()

    base_path = Path(base_path)/ project_name
    base_path.mkdir(parents=True, exist_ok=True)
    logs_path = base_path  / 'logs'
    logs_path.mkdir(exist_ok=True)

    return base_path
project_root = create_project_directories(BASE_PATH, PROJECT_NAME)
log_dir = project_root / 'logs' 

# ------------------------------------------------------
# Создаем систему логирования.
# ------------------------------------------------------

def setup_logging():
           
    # Текущая дата для имени файла
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(log_dir, f"{current_date}.log")
    
    # Настройка базовой конфигурации логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8')]          
    )
    
    # Удаление старых логов (старше 3 дней)
    cleanup_old_logs(log_dir)

    logger = logging.getLogger('logger')
    logger.log_file = log_file
    logger.log_dir = log_dir
    return logger

def cleanup_old_logs(log_dir, days_to_keep=3):    # Удаление старых логов (старше 3 дней)
    
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    
    for filename in os.listdir(log_dir):
        if filename.endswith('.log'):
            file_path = os.path.join(log_dir, filename)
            
            try:
                file_date_str = filename.replace('.log', '')
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                
                if file_date < cutoff_date:
                    os.remove(file_path)
                    print(f"Удален старый лог: {filename}")
            except ValueError:
                continue

# Инициализация логирования при импорте модуля
logger = setup_logging()

# ------------------------------------------------------
# Создаем подключение и выгружаем данные в питон-формат
# ------------------------------------------------------
logger.info("===============================================")
logger.info("Начинаем скачивание данных из API...")

try:
    r = requests.get(API_URL, params=params)
    logger.info(f"Статус ответа API: {r.status_code}")
    
    if r.status_code != 200:
        logger.error(f"Ошибка доступа к API. Status code: {r.status_code}")
        raise Exception(f"API request failed with status {r.status_code}")
    
    raw_data = r.json()
    logger.info(f"Данные успешно загружены. Получено записей: {len(raw_data)}")
    
except Exception as e:
    logger.error(f"Неожиданная ошибка при загрузке данных: {str(e)}")
    raise

# ------------------------------------------------------
# Проверяем валидность данных и подготавливаем к заливке в базу
# ------------------------------------------------------
logger.info("Начинаем валидацию данных")

class RecordValidator:
    def __init__(self, logger):
        self.logger = logger
        self.errors = []
        self.valid_records = []
        self.statistics = {
            'total_records': 0,
            'valid_records': 0,
            'invalid_records': 0,
            'errors_by_type': {}
        }
    
    # Проверяем user_id
    def validate_user_id(self, user_id: Any):
        if not user_id:
            return None, "user_id is empty"
        
        user_id_str = str(user_id)
        if re.match(r'^[a-f0-9]{32}$', user_id_str.lower()):
            return user_id_str, None
        else:
            return None, f"Invalid user_id format: {user_id_str}"
    
    # Проверяем date
    def validate_date(self, date: Any):
        if not date:
            return None, "date is empty"
        try:
            date_str = str(date)
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
            if parsed_date > datetime.now():
                return None, f"Date is in the future: {date_str}"
            return date_str, None
        except ValueError:
            return None, f"Invalid date format: {date}."
    
    # Проверяем attempt_type (ожидается 'run' или 'submit')  
    def validate_attempt_type(self, attempt_type: Any):
        if attempt_type is None:
            return None, "attempt_type is required (cannot be null)"
        
        attempt_type_str = str(attempt_type).strip().lower()
        if not attempt_type_str:
            return None, "attempt_type is empty"
        
        valid_types = ['run', 'submit'] 
        if attempt_type_str in valid_types:
            return attempt_type_str, None
        else:
            return None, f"Invalid attempt_type (expected one of {valid_types}, got: {attempt_type})"

    # Проверяем is_correct
    def validate_is_correct(self, is_correct: Any, attempt_type):
                
        if attempt_type == 'run':
            if is_correct is None:
                return None, None
            else:
                return None, f"for attempt_type='run', is_correct must be null, got: {is_correct}"
        
        elif attempt_type == 'submit':
            if is_correct is None:
                return None, "for attempt_type='submit', is_correct cannot be null"
            
            if isinstance(is_correct, bool):
                return is_correct, None
            elif isinstance(is_correct, (int, float)):
                return bool(is_correct), None
            elif isinstance(is_correct, str):
                lower_val = is_correct.lower().strip()
                if lower_val in ('true', '1', 'yes', 'y'):
                    return True, None
                elif lower_val in ('false', '0', 'no', 'n'):
                    return False, None
                else:
                    return None, f"Invalid is_correct value for submit (expected boolean, got: {is_correct})"
            else:
                return None, f"Invalid is_correct type for submit (expected boolean, got: {type(is_correct)})"
        
        else:
        # Если attempt_type неизвестен, возвращаем ошибку
            return None, f"Unknown attempt_type: {attempt_type}"
        
    # Парсим строку passback_params в словарь   
    def parse_passback_params(self, passback_params_str: str):
        if not passback_params_str:
            return None, "passback_params is empty"
        try:
            params_dict = ast.literal_eval(passback_params_str)
            if isinstance(params_dict, dict):
                return params_dict, None
            else:
                return None, f"Parsed value is not a dictionary: {type(params_dict)}"
        except (SyntaxError, ValueError) as e:
            return None, f"Failed to parse passback_params: {str(e)}"
    
    def validate_passback_params_dict(self, params_dict: Dict):
        
        errors = []
        
        # Проверяем наличие обязательных полей
        required_fields = ['oauth_consumer_key', 'lis_result_sourcedid', 'lis_outcome_service_url']
        for field in required_fields:
            if field not in params_dict:
                errors.append(f"Missing required field in passback_params: {field}")
            elif params_dict[field] is None:
                errors.append(f"Field {field} in passback_params is null")
        
        # Проверяем формат lis_result_sourcedid
        if 'lis_result_sourcedid' in params_dict and params_dict['lis_result_sourcedid']:
            sourcedid = params_dict['lis_result_sourcedid']
            # Можно добавить специфическую валидацию формата sourcedid
            if not isinstance(sourcedid, str) or ':' not in sourcedid:
                errors.append(f"Invalid lis_result_sourcedid format: {sourcedid}")
        
        # Проверяем URL lis_outcome_service_url
        if 'lis_outcome_service_url' in params_dict and params_dict['lis_outcome_service_url']:
            url = params_dict['lis_outcome_service_url']
            url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            if not re.match(url_pattern, str(url)):
                errors.append(f"Invalid lis_outcome_service_url format: {url}")
        
        return errors
    
    def process_record(self, record: Dict):
        valid_record = {}
        record_errors = []

        self.statistics['total_records'] += 1     
        
        # Проверяем user_id
        user_id, error = self.validate_user_id(record.get('lti_user_id'))
        if user_id:
            valid_record['user_id'] = user_id
        else:
            record_errors.append(f"user_id: {error}")
        
        # Проверяем date(created_at)
        date, error = self.validate_date(record.get('created_at'))
        if date:
            valid_record['date'] = date
        else:
            record_errors.append(f"date: {error}")

        # Валидация is_correct
        is_correct, error = self.validate_is_correct(record.get('is_correct'), 
    record.get('attempt_type'))
        if error is None:
            valid_record['is_correct'] = is_correct
        else:
            record_errors.append(f"is_correct: {error}")
        
        # Валидация attempt_type
        attempt_type, error = self.validate_attempt_type(record.get('attempt_type'))
        if attempt_type:
            valid_record['attempt_type'] = attempt_type
        else:
            record_errors.append(f"attempt_type: {error}")
        
        # Парсим и валидируем passback_params
        passback_params_dict, error = self.parse_passback_params(record.get('passback_params'))
        if passback_params_dict:
            
            param_errors = self.validate_passback_params_dict(passback_params_dict)
            if param_errors:
                record_errors.extend([f"passback_params: {e}" for e in param_errors])
            else:
                # Сохраняем распарсенный словарь
                valid_record['passback_params'] = passback_params_dict
                
                # Извлекаем отдельные поля для удобства
                valid_record['oauth_consumer_key'] = passback_params_dict.get('oauth_consumer_key')
                valid_record['lis_result_sourcedid'] = passback_params_dict.get('lis_result_sourcedid')
                valid_record['lis_outcome_service_url'] = passback_params_dict.get('lis_outcome_service_url')
        else:
            record_errors.append(f"passback_params: {error}")
        
        # Если есть ошибки
        if record_errors:
            self.statistics['invalid_records'] += 1
            #self.statistics['errors_by_type'][len(record_errors)] = \
              #  self.statistics['errors_by_type'].get(len(record_errors), 0) + 1
            
            self.errors.append({
                'original_record': record,
                'errors': record_errors
            })
            return None
        else:
            self.statistics['valid_records'] += 1
            return valid_record
     
        
    # Обрабатываем сырые скачанные данные
    def process_records(self, raw_data):
        self.valid_records = []
        self.statistics['total_records'] = len(raw_data) if raw_data else 0
        self.logger.info(f"Начинаем валидацию {self.statistics['total_records']} записей")

        for record in raw_data:
            valid_record = self.process_record(record)
            if valid_record:
                self.valid_records.append(valid_record)
        self.logger.info(f"Валидация завершена. Валидных записей: {self.statistics['valid_records']}, невалидных: {self.statistics['invalid_records']}")
        return self.valid_records
    
    # Сохраняем ошибки в файл
    def save_errors(self, filename: str = None, file_path: str = None):
        save_path = project_root

        if filename is None:
           current_date = datetime.now().strftime('%Y%m%d')
           filename = f'errors_{current_date}.txt'
     
        file_path = save_path / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("СТАТИСТИКА ОШИБОК\n")
            f.write(f"Всего записей с ошибками: {len(self.errors)}\n")
            
            for error_item in self.errors:
                f.write(f"Record: {error_item['original_record']}\n")
                for error in error_item['errors']:
                    f.write(f"  - {error}\n")
                f.write("-" * 50 + "\n")
        self.logger.info(f"Ошибки сохранены в файл: {filename}")
        self.logger.info(f"Всего записей с ошибками: {len(self.errors)}")
    
validator = RecordValidator(logger)
valid_records = validator.process_records(raw_data)

validator.save_errors()

# ------------------------------------------------------
# Сохраняем записи в csv файл
# ------------------------------------------------------

def save_to_csv(valid_records, filename='training_data.csv', custom_path=None):
        
    if not valid_records:
        logger.info("Нет валидных записей для сохранения")
        print("Нет валидных записей для сохранения")
        return False
    
    if custom_path is None:
        save_dir = project_root 
    else:
       save_dir = Path(custom_path)
    
    # Создаем директорию, если её нет
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # Полный путь к файлу
    file_path = save_dir / filename
    
    # Определяем поля для CSV
    fieldnames = ['user_id',  
                  'oauth_consumer_key', 'lis_result_sourcedid', 
                  'lis_outcome_service_url', 'is_correct', 'attempt_type', 'created_at']
    
    try:
        logger.info(f"Начинаем сохранение {len(valid_records)} записей в CSV файл")
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Записываем заголовок
            writer.writeheader()
            
            # Записываем данные
            for record in valid_records:
                csv_record = {
                    'user_id': record.get('user_id'),
                    'oauth_consumer_key': record.get('oauth_consumer_key'),
                    'lis_result_sourcedid': record.get('lis_result_sourcedid'),
                    'lis_outcome_service_url': record.get('lis_outcome_service_url'),
                    'is_correct': record.get('is_correct'),
                    'attempt_type': record.get('attempt_type'),
                    'created_at': record.get('date')
                }
                writer.writerow(csv_record)
        
        logger.info(f"Данные успешно сохранены в {file_path}")
        logger.info(f"Всего записей: {len(valid_records)}")
        
        return file_path  
         
    except Exception as e:
        logger.error(f" Ошибка при сохранении в CSV: {e}")
        return False
    
# Сохраняем данные в CSV
csv_filename = 'training_data.csv'
csv_file_path = save_to_csv(valid_records, csv_filename)

# ------------------------------------------------------
# Импорт данных из csv в базу
# ------------------------------------------------------

def import_csv_to_postgresql(csv_file_path, db_config):
    logger.info("Начинаем импортировать данные в POSTGRESQL")
    conn = None
    csv_path = Path(csv_file_path)

    if not csv_path.exists():
        logger.error(f"CSV файл не найден: {csv_path}")
        print(f"CSV файл не найден: {csv_path}")
        return False
       
    try:
        logger.info(f"Подключаемся к PostgreSQL")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        logger.info("Подключение к PostgreSQL успешно установлено")

        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'training_data'
            );
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            logger.info("Таблица не существует. Создаем новую таблицу...")
            
            create_table_query = """
            CREATE TABLE training_data (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(32) NOT NULL,
                oauth_consumer_key TEXT,
                lis_result_sourcedid TEXT,
                lis_outcome_service_url TEXT,
                is_correct BOOLEAN,
                attempt_type VARCHAR(10) NOT NULL,
                created_at TIMESTAMP NOT NULL,
                -- Добавляем уникальное ограничение для предотвращения дубликатов
                CONSTRAINT unique_user_attempt UNIQUE (user_id, created_at, attempt_type)
            );
            
            -- Создаем индексы для ускорения поиска
            CREATE INDEX idx_training_user_id ON training_data(user_id);
            CREATE INDEX idx_training_created_at ON training_data(created_at);
            """
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Таблица успешно создана")
        else:
            logger.info("Таблица уже существует")
        
        # Получаем существующие записи для проверки дубликатов
        logger.info("Проверяем существующие записи для предотвращения дубликатов...")
        cur.execute("""
            SELECT user_id, created_at, attempt_type 
            FROM training_data
        """)
        existing_records = set()
        for row in cur.fetchall():
            # Создаем ключ для проверки дубликатов
            existing_records.add((row[0], str(row[1]), row[2]))
        
        logger.info(f"Найдено существующих записей: {len(existing_records)}")
        
        # Читаем CSV и фильтруем новые записи
        new_records = []
        duplicates_skipped = 0
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            
            for row in csv_reader:
                # Создаем ключ для проверки
                record_key = (
                    row['user_id'],
                    row['created_at'],
                    row['attempt_type']
                )
                
                # Проверяем, существует ли уже такая запись
                if record_key in existing_records:
                    duplicates_skipped += 1
                    continue
                
                new_records.append(row)
        
        logger.info(f"Новых записей для импорта: {len(new_records)}")
        logger.info(f"Пропущено дубликатов: {duplicates_skipped}")
        
        # Импортируем только новые записи
        if new_records:
            logger.info("Начинаем импорт новых записей в PostgreSQL...")
            temp_csv = csv_path.parent / 'temp_import.csv'
            with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['user_id', 'oauth_consumer_key', 
                                                       'lis_result_sourcedid', 'lis_outcome_service_url',
                                                       'is_correct', 'attempt_type', 'created_at'])
                writer.writeheader()
                writer.writerows(new_records)
            
            # Импортируем новые записи
            with open(temp_csv, 'r', encoding='utf-8') as f:
                next(f)  # Пропускаем заголовок
                cur.copy_expert("""
                    COPY training_data 
                    (user_id, oauth_consumer_key, lis_result_sourcedid, 
                     lis_outcome_service_url, is_correct, attempt_type, created_at)
                    FROM STDIN WITH CSV
                """, f)
            
            conn.commit()
            logger.info(f"Успешно импортировано {len(new_records)} новых записей")
            print(f"Импортировано {len(new_records)} новых записей")
            
            # Удаляем временный файл
            os.remove(temp_csv)
            logger.debug(f"Временный файл {temp_csv} удален")
        else:
            logger.info("Нет новых записей для импорта")
            print("ℹ Нет новых записей для импорта")
           
             
        # Показываем итоговую статистику
        cur.execute("SELECT COUNT(*) FROM training_data")
        total_count = cur.fetchone()[0]
        logger.info(f"Всего записей в таблице: {total_count}")
        
        cur.close()

        logger.info("Импорт в базу успешно завершен")
       
        return True

    except psycopg2.OperationalError as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        return False

    except Exception as e:
        logger.error(f"Ошибка при импорте в PostgreSQL: {e}")
        if conn:
            conn.rollback()
            logger.info("Транзакция отменена (rollback)")
        return False
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с PostgreSQL закрыто")

if csv_file_path:
    imported_count = import_csv_to_postgresql(csv_file_path, DB_CONFIG)

    if imported_count is not False:
        logger.info("Процесс успешно завершен")
        print("Процесс успешно завершен")

        # Удаляем CSV файл после успешного импорта
        os.remove(csv_file_path)
        logger.info(f"CSV файл удален: {csv_file_path}")
                
logger.info(f"Лог сохранен в: {logger.log_file}")
print(f"Лог-файл и ошибки сохранены в: {Path(project_root)}")

