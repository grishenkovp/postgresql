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
            """CREATE TABLE IF NOT EXISTS plan (id INTEGER,manager TEXT, val_plan INTEGER);
            CREATE TABLE IF NOT EXISTS fact (id INTEGER,manager TEXT, val_fact INTEGER);"""
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
    """Добавить значения в таблицы"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sales_plan = [(1, 'm1', 10), (2, 'm2', 20), (3, 'm3', 30)]
        sales_fact = [(1, 'm1', 15), (2, 'm2', 19), (3, 'm3', 35)]

        sales_plan_records = ", ".join(["%s"] * len(sales_plan))
        sales_fact_records = ", ".join(["%s"] * len(sales_fact))

        cursor.execute(f"INSERT INTO plan (id, manager, val_plan) VALUES {sales_plan_records}", sales_plan)
        cursor.execute(f"INSERT INTO fact (id, manager, val_fact) VALUES {sales_fact_records}", sales_fact)

        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")


def func_query_fact() -> None:
    """Функция, позволяющая подтягивать значения из таблицы факт в таблицу план по id менеджера"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        conn = psycopg2.connect(point)
        sql = """CREATE OR REPLACE FUNCTION public.query_fact(integer)
                        RETURNS integer AS
                 $BODY$
                        SELECT f.val_fact 
                        FROM public.fact as f 
                        WHERE id = $1;
                 $BODY$
                       LANGUAGE 'sql' VOLATILE"""

        cursor.execute(sql)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")

def func_query_result_sale() -> None:
    """Функция, позволяющая проверять соотношение факт-план по каждому менеджеру"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sql = """CREATE OR REPLACE FUNCTION public.query_result_sale(integer, integer)
               RETURNS text AS
       $BODY$
            DECLARE
               val_fact ALIAS FOR $1;
               val_plan ALIAS FOR $2;
               delta integer;
               result text;
            BEGIN
                     
            delta:=(val_fact - val_plan);
            
            IF (delta > 0) or (delta = 0) THEN
               result := 'План выполнен'; 
            ELSE 
               result := 'План не выполнен';
            END IF;
            
            RETURN result;
            
            END;
       $BODY$
            LANGUAGE 'plpgsql' VOLATILE"""
        cursor.execute(sql)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")


def func_query_report_tabl() -> None:
    """Функция, выводящая только менеджеров, которые выполнили план продаж. Создание"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        sql = """CREATE OR REPLACE FUNCTION public.query_report_tabl(param text)
                            RETURNS TABLE(id INTEGER, 
                                          manager TEXT, 
                                          val_plan INTEGER, 
                                          val_fact INTEGER, 
                                          result TEXT) AS 
                $BODY$
                    BEGIN
                    RETURN QUERY
                        SELECT p.*, 
                              public.query_fact(p.id) AS val_fact ,
                              public.query_result_sale(public.query_fact(p.id),p.val_plan) AS result
                        FROM public.plan AS p
                        WHERE public.query_result_sale(public.query_fact(p.id),p.val_plan) = param;
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

def func_query_report_tabl_result() -> None:
    """Функция, выводящая только менеджеров, которые выполнили план продаж. Вывод результатов"""
    try:
        conn = psycopg2.connect(point)
        cursor = conn.cursor()
        cursor.callproc('public.query_report_tabl', ['План выполнен', ])
        result = cursor.fetchall()
        col_name = ['id', 'manager', 'val_plan', 'val_fact', 'result']
        print(pd.DataFrame(result, columns=col_name))
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
sql1 = """select f.* from public.fact as f"""
sql2 = """select p.*, public.query_fact(p.id) as val_fact from public.plan as p"""
sql3 = """select p.*, 
                public.query_fact(p.id) as val_fact ,
                public.query_result_sale(public.query_fact(p.id),p.val_plan) as result
                from public.plan as p"""

if __name__ == '__main__':
    # func_query_report_tabl_result()
    print(select_postgresql(sql3))
