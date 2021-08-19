import psycopg2
from psycopg2 import Error
import pandas as pd

user = 'postgres'
password = 'admin'
host = 'localhost'
post = 5432
name = 'test'

point = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, post, name)


def create_tables() -> None:
    """Создать таблицы в БД """
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS sales (id INTEGER,manager TEXT, val INTEGER);"""
        )
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")


def insert_values() -> None:
    """Добавить значения в таблицу"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sales = [(1, 'm1', 10), (2, 'm2', 20), (3, 'm3', 30), (4, 'm4', 40), (5, 'm5', 50)]
        sales_records = ", ".join(["%s"] * len(sales))
        cursor.execute(f"INSERT INTO sales (id, manager, val) VALUES {sales_records}", sales)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")


def func_query_filter_table() -> None:
    """Функция, позволяющая фильтровать таблицу sales"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sql = """CREATE OR REPLACE FUNCTION public.query_filter_table (param integer)
                 RETURNS TABLE (id integer, manager text, val integer) AS
                 $BODY$ 
                    BEGIN
                    RETURN QUERY
                        select s.* from public.sales as s where s.val = param;
                    END;
                $BODY$
                        LANGUAGE plpgsql;"""

        cursor.execute(sql)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

def func_query_filter_table_loop() -> None:
    """Функция, позволяющая фильтровать таблицу sales"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sql = """CREATE OR REPLACE FUNCTION public.query_filter_table_loop ()
                 RETURNS TABLE (id integer, manager text, val integer) AS
                 $BODY$ 
                    DECLARE
                          rec RECORD;
                    BEGIN
                        FOR rec IN EXECUTE 'select s.* from public.sales as s where s.val = 10'
                            LOOP
                                id = rec.id;
                                manager =rec.manager;
                                val = rec.val;
                                RETURN next;
                            END LOOP;
                    END;
                $BODY$
                    LANGUAGE 'plpgsql' VOLATILE
                    COST 100
                    ROWS 1000;
                ALTER FUNCTION public.query_filter_table_loop() OWNER TO postgres;"""

        cursor.execute(sql)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

def select_postgresql(sql: str):
    """Запрос данных из БД"""
    conn = psycopg2.connect(point)
    return pd.read_sql(sql, conn)


# Промежуточный контроль результатов
sql1 = """select s.* from public.sales as s"""
sql2 = """select ft.* from public.query_filter_table(10) as ft"""
sql3 = """select ft.* from public.query_filter_table_loop() as ft"""

if __name__ == '__main__':
    # func_query_filter_table_loop()
    print(select_postgresql(sql3))

