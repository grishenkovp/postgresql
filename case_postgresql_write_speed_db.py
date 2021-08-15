import pandas as pd
import random
import psycopg2
import os
import time
from datetime import datetime
from tqdm import tqdm
from glob import glob
from typing import List

# ************************************** Генерация датасетов *****************************************

list_date = []
list_manager = []
list_product = []
list_val = []
list_name_manager = ['m1', 'm2', 'm3']
list_name_product = ['pr1', 'pr2', 'pr3', 'pr4', 'pr5']

path_data_all = 'data_all/sales.csv'


def create_dataframe(size: int, path_data_all: str = path_data_all) -> None:
    """Генератор датасета"""
    for _ in tqdm(range(size)):
        list_manager.append(list_name_manager[random.randint(0, 2)])
        list_product.append(list_name_product[random.randint(0, 4)])
        list_date.append(datetime.now().date())
        list_val.append(random.randint(100, 1000))

    df = pd.DataFrame({'dt': list_date,
                       'manager': list_manager,
                       'product': list_product,
                       'val': list_val})
    df.to_csv(path_data_all, index=False, sep=',')


chunk_size = 500000
batch_num = 1


def create_dataframe_chunk() -> None:
    """Генератор датасетов по 500000 строк"""
    global batch_num
    parser = lambda dt: datetime.strptime(dt, '%Y-%m-%d')
    for chunk in pd.read_csv("data_all/sales.csv",
                             parse_dates=['dt'],
                             date_parser=parser,
                             chunksize=chunk_size):
        chunk.to_csv('data_chunk/sales' + str(batch_num) + '.csv', index=False, sep=',')
        batch_num = batch_num + 1


# ************************************** Формирование таблиц в БД *****************************************
user = 'postgres'
password = 'admin'
host = 'localhost'
post = 5432
name = 'test'

point = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, post, name)

conn = psycopg2.connect(point)

list_tabl_name = ['sales_all', 'sales_chunk']


def create_tables(list_tabl_name: List = list_tabl_name) -> None:
    """Создать таблицу"""
    cursor = conn.cursor()
    for tabl_name in list_tabl_name:
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {tabl_name} (id SERIAL PRIMARY KEY,dt DATE NOT NULL,manager TEXT NOT NULL,"
            f"product TEXT NOT NULL, val REAL NOT NULL);"
        )
        conn.commit()
    cursor.close()
    conn.close()

# ************************************** Тестирование скорости записи *****************************************
columns_name = ['dt', 'manager', 'product', 'val']


def load_data_all(tabl_name: str = 'sales_all',
                  path_data_all: str = path_data_all,
                  columns_name: List = columns_name,
                  sep: str = ',') -> None:
    """Записать данные в таблицу"""
    cursor = conn.cursor()
    with open(path_data_all, 'r') as f:
        next(f)
        cursor.copy_from(f, tabl_name, sep=sep, columns=columns_name)
        conn.commit()
        f.close()
    cursor.close()
    conn.close()


path_data_chunk = 'data_chunk'


def get_all_file(path_data_chunk: str = path_data_chunk) -> List:
    """Получить список файлов в папке"""
    all_file_name = list(glob(os.path.join(path_data_chunk, '*.csv')))
    return all_file_name


def load_data_chunk(table_name: str = 'sales_chunk',
              sep: str = ',',
              columns_name: list = columns_name) -> None:
    """Записать данные в таблицу"""
    cursor = conn.cursor()
    for _ in get_all_file():
        with open(_, 'r') as f:
            next(f)
            cursor.copy_from(f, table_name, sep=sep, columns=columns_name)
            conn.commit()
            f.close()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    start_time = time.time()
    load_data_chunk()
    finish_time = time.time()
    print("--- %s seconds ---" % (finish_time - start_time))
