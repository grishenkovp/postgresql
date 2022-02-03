import pandas as pd
from sqlalchemy import create_engine
import yaml

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

user = settings['DB']['USER']
password = settings['DB']['PASSWORD']
host = settings['DB']['HOST']
post = settings['DB']['PORT']
name = settings['DB']['NAME']

path_to_data = "dataset.csv"
postgresql_table = 'test_tbl'

point_connect = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, post, name)
con = create_engine(point_connect)


df = pd.read_csv(path_to_data)

# В БД CREATE DATABASE test / CREATE TABLE test_tbl (...)

df.to_sql(postgresql_table, point_connect, if_exists='replace', index=False)


def select_postgresql(sql):
    return pd.read_sql(sql, con)


# Вопрос 1. ...
sql1 = '''select tbl.* from test_tbl as tbl'''

print(select_postgresql(sql1))
