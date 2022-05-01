### Алгоритм работы с библиотекой Alembic
###
* Файл db.py: подготовить описание таблиц для БД
* Создать БД
* Терминал: alembic init migration
* Файл env.py: target_metadata = Base.metadata (из файла db.py)
* Файл alembic.ini: sqlalchemy.url = driver://user:pass@localhost/dbname (указываем правильный путь к БД)
* Терминал: alembic revision --autogenerate -m 'initial'
* Терминал: alembic upgrade head