/* Источник db1 / tbl1 (val_1 int, val_2 int)
Приемник db2 / tbl2 (val_2 int, val_2 int) */


SELECT * FROM pg_catalog.pg_available_extensions;

--Установите расширение postgres_fdw
CREATE EXTENSION postgres_fdw;

--Создайте объект стороннего сервера, используя CREATE SERVER, который будет представлять удалённую базу данных, 
--к которой вы хотите подключаться. Укажите свойства подключения, кроме user и password, в параметрах объекта сервера.
CREATE SERVER postgres_fdw_test FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'db1', port '5432');

--Создайте сопоставление пользователей, используя CREATE USER MAPPING, для каждого пользователя базы, 
--которому нужен доступ к удалённому серверу. Укажите имя и пароль удалённого пользователя в параметрах 
--user и password сопоставления.
CREATE USER MAPPING FOR postgres SERVER postgres_fdw_test
OPTIONS (user 'postgres', password 'nopswd');


--Создайте стороннюю таблицу, используя CREATE FOREIGN TABLE или IMPORT FOREIGN SCHEMA, для каждой удалённой таблицы, 
--к которой вы хотите обращаться. Столбцы сторонней таблицы должны соответствовать столбцам целевой удалённой таблицы. 
--Однако вы можете использовать локально имена таблиц и/или столбцов, отличные от удалённых, 
--если укажете корректные удалённые имена в параметрах объекта сторонней таблицы.
CREATE FOREIGN TABLE tbl2 (val_1 int, val_2 int) SERVER postgres_fdw_test
OPTIONS (table_name 'tbl1');

SELECT * FROM tbl2 t 