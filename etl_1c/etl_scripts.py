import os
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import yaml
from typing import List

# pd.set_option('display.max_columns', None)
# pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.max_colwidth', 80)
# pd.set_option('display.max_rows', 50)
# pd.get_option('display.precision', 2)
# pd.set_option('display.float_format', '{:.2f}'.format)

# Folder paths
PATH_DATASETS = 'datasets'
PATH_DATASETS_RESULT = 'datasets_result'

# Filenames
SALES = 'sales.txt'
NOMENCLATURE = 'nomenclature.xls'
COUNTERPARTIES = 'counterparties.txt'
CONTRACTS = 'contracts.txt'
SALES_RESULT = 'sales.csv'
NOMENCLATURE_RESULT = 'nomenclature.csv'
COUNTERPARTIES_RESULT = 'counterparties.csv'
CONTRACTS_RESULT = 'contracts.csv'

# DWH
DB_SCHEMA = 'src'
DB_TABLE_SALES = 'sales'
DB_TABLE_CONTRACTS = 'contracts'
DB_TABLE_COUNTERPARTIES = 'counterparties'
DB_TABLE_NOMENCLATURE = 'nomenclature'

# Paths to files
path_dataset_sales = os.path.join(PATH_DATASETS, SALES)
path_dataset_nomenclature = os.path.join(PATH_DATASETS, NOMENCLATURE)
path_dataset_counterparties = os.path.join(PATH_DATASETS, COUNTERPARTIES)
path_dataset_contracts = os.path.join(PATH_DATASETS, CONTRACTS)

path_dataset_sales_result = os.path.join(PATH_DATASETS_RESULT, SALES_RESULT)
path_dataset_nomenclature_result = os.path.join(PATH_DATASETS_RESULT, NOMENCLATURE_RESULT)
path_dataset_counterparties_result = os.path.join(PATH_DATASETS_RESULT, COUNTERPARTIES_RESULT)
path_dataset_contracts_result = os.path.join(PATH_DATASETS_RESULT, CONTRACTS_RESULT)

# --- PostgreSQL ---

with open('settings.yaml', encoding='utf8') as f:
    settings = yaml.safe_load(f)

user = settings['DB']['USER']
password = settings['DB']['PASSWORD']
host = settings['DB']['HOST']
post = settings['DB']['PORT']
name = settings['DB']['NAME']

point_connect = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, post, name)
con = create_engine(point_connect)

# --- Продажи ---

column_names_dataset_sales = ['dt', 'accounting_document', 'company',
                              'counterparty', 'contract', 'accounting_document_',
                              'quantity', 'nomenclature_group', 'tax_rate',
                              'nomenclature', 'amount', 'contents_operation']

dateparse = lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M:%S')
df_sales = pd.read_csv(path_dataset_sales,
                       sep='\t',
                       header=0,
                       names=column_names_dataset_sales,
                       parse_dates=['dt'],
                       date_parser=dateparse,
                       )

df_sales = df_sales[df_sales['contents_operation'].str.contains('Реализация товаров|Реализация продукции')]
df_sales = df_sales[df_sales['accounting_document'].str.contains('акт, накладная, УПД')]


def convert_string_to_number(s: str) -> int:
    try:
        return int(s.replace(chr(160), '').replace(',', '.'))
    except:
        return int(float(s.replace(chr(160), '').replace(',', '.')))


df_sales['quantity'] = df_sales['quantity'].apply(lambda el: convert_string_to_number(el))
df_sales['amount'] = df_sales['amount'].apply(lambda el: convert_string_to_number(el))


def remove_extra_spaces_in_string(df_current: pd.DataFrame, col_names: List) -> None:
    try:
        for col in col_names:
            df_current[col] = df_current[col].apply(lambda s: ' '.join(s.split()))
    except Exception:
        print(f'Столбец {col} не получается очистить от лишних пробелов')


column_names_dataset_sales_str = df_sales.select_dtypes(include=['object']).columns.tolist()
remove_extra_spaces_in_string(df_sales, column_names_dataset_sales_str)

df_sales_result = df_sales[['dt',
                            'company',
                            'counterparty',
                            'contract',
                            'nomenclature',
                            'quantity',
                            'amount']]

# print(df_sales_result.head())
df_sales_result.to_csv(path_dataset_sales_result, index=False)
df_sales_result.to_sql(DB_TABLE_SALES, con=con, schema=DB_SCHEMA, if_exists='append', index=False)

# --- Компании ---

column_names_dataset_counterparties = ['company',
                                       'taxpayer_identification_number',
                                       'full_name_company']

column_dtypes_dataset_counterparties = {'company': str,
                                        'taxpayer_identification_number': str,
                                        'full_name_company': str}
df_companies = pd.read_csv(path_dataset_counterparties,
                           sep='\t',
                           header=0,
                           names=column_names_dataset_counterparties,
                           dtype=column_dtypes_dataset_counterparties)

column_names_dataset_companies_str = df_companies.select_dtypes(include=['object']).columns.tolist()
column_names_dataset_companies_str.remove('taxpayer_identification_number')
remove_extra_spaces_in_string(df_companies, column_names_dataset_companies_str)

# print(df_companies.head())
df_companies.to_csv(path_dataset_counterparties_result, index=False)
df_companies.to_sql(DB_TABLE_COUNTERPARTIES, con=con, schema=DB_SCHEMA, if_exists='append', index=False)

# --- Договоры ---

column_names_dataset_contracts = ['contract', 'counterparty', 'type_contract', 'currency', 'company']

df_contracts = pd.read_csv(path_dataset_contracts,
                           sep='\t',
                           header=0,
                           names=column_names_dataset_contracts)

column_names_dataset_contracts_str = df_contracts.select_dtypes(include=['object']).columns.tolist()
remove_extra_spaces_in_string(df_contracts, column_names_dataset_contracts_str)

# print(df_contracts.head())
df_contracts.to_csv(path_dataset_contracts_result, index=False)
df_contracts.to_sql(DB_TABLE_CONTRACTS, con=con, schema=DB_SCHEMA, if_exists='append', index=False)

# --- Номенклатура ---

column_names_dataset_nomenclature = ['nomenclature']

df_nomenclature = pd.read_excel(path_dataset_nomenclature,
                                header=0,
                                names=column_names_dataset_nomenclature,
                                usecols=[0],
                                skiprows=1)
column_names_dataset_nomenclature_str = df_nomenclature.select_dtypes(include=['object']).columns.tolist()
remove_extra_spaces_in_string(df_nomenclature, column_names_dataset_nomenclature_str)

# print(df_nomenclature.head())
df_nomenclature.to_csv(path_dataset_nomenclature_result, index=False)
df_nomenclature.to_sql(DB_TABLE_NOMENCLATURE, con=con, schema=DB_SCHEMA, if_exists='append', index=False)
