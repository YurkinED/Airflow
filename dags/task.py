import csv
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

BASE_FILE_PATH = "dags/files/"
DAG_ID = "ETL_task"
EXPORT_FILENAME = 'export_data.csv'

dag_params = {
    'dag_id': DAG_ID,
    'start_date': datetime(2021, 5, 28),
    'schedule_interval': None
}

def export_mysql():
    # Get the hook
    pgsqlserver = PostgresHook("pg_data")
    connection = pgsqlserver.get_conn()
    cursor = connection.cursor()
    mysql = MySqlHook('mysql_data')
    mysql.run('USE test;')
    res=mysql.get_records('select coalesce(max(id),0) as cnt from test.raw_order;')
    print('Using max id for insert data: {}'.format(res[0][0]))
    # Execute the query
    cursor.execute('''
                SELECT ID, STUDENT_ID, TEACHER_ID, STAGE, STATUS, CREATED_AT, UPDATED_AT FROM ORDER_TBL WHERE ID>{}
                '''.format(res[0][0]))
    sources = cursor.fetchall()
    mysql.insert_rows(table='test.raw_order', rows=sources)
    print("Export done")

def export_csv_to_mysql():
    tmp_path = BASE_FILE_PATH + EXPORT_FILENAME
    Data = pd.read_csv(tmp_path, delimiter=';')
    print(Data)
    mysql = MySqlHook('mysql_data')
    connection = mysql.get_conn()
    cursor = connection.cursor()
    mysql.run('USE test;')
    mysql.run('truncate table test.raw_order;')
    for i,row in Data.iterrows():
        sql = '''INSERT INTO TEST.RAW_ORDER(
        ORDER_ID, 
        STUDENT_ID,
        TEACHER_ID,
        STAGE,
        STATUS,
        CREATED_AT,
        UPDATED_AT) 
        VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s)'''
        cursor.execute(sql, tuple(row))
        print("Record inserted")
    connection.commit()
    print("Export done") 

def mysql_remove_dublicates():
    tmp_path = BASE_FILE_PATH + EXPORT_FILENAME
    Data = pd.read_csv(tmp_path, delimiter=';')
    print(Data)
    mysql = MySqlHook('mysql_data')
    connection = mysql.get_conn()
    cursor = connection.cursor()
    mysql.run('USE test;')
    mysql.run('TRUNCATE TABLE TEST.RAW_ORDER_FINAL;')
    mysql.run('''INSERT INTO TEST.RAW_ORDER_FINAL 
                            (ORDER_ID, 
                            STUDENT_ID, 
                            TEACHER_ID, 
                            STAGE, 
                            STATUS, 
                            ROW_HASH, 
                            CREATED_AT, 
                            UPDATED_AT) 
                            SELECT 
                            ORDER_ID, 
                            STUDENT_ID, 
                            TEACHER_ID, 
                            STAGE, 
                            STATUS, 
                            ROW_HASH,
                            CREATED_AT, 
                            UPDATED_AT 
                            FROM  TEST.RAW_ORDER;''')
    connection.commit()
    print("Rempve dublicates done")        
    
    
    
def export_to_csv():
    # Get the hook
    pgsqlserver = PostgresHook("pg_data")
    connection = pgsqlserver.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT ID, STUDENT_ID, TEACHER_ID, STAGE, STATUS, CREATED_AT, UPDATED_AT FROM ORDER_TBL")
    result = cursor.fetchall()
    tmp_path = BASE_FILE_PATH + EXPORT_FILENAME
    with open(tmp_path, 'w') as fp:
        a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = ';')
        a.writerow([i[0] for i in cursor.description])
        a.writerows(result)
    print("Export done")



with DAG(**dag_params) as dag:
    prepare_create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='pg_data',
        sql='''CREATE TABLE IF NOT EXISTS ORDER_TBL(
                ID BIGINT PRIMARY KEY,
                STUDENT_ID BIGINT,
                TEACHER_ID BIGINT,
                STAGE VARCHAR(10),
                STATUS VARCHAR(512),
                CREATED_AT TIMESTAMP,
                UPDATED_AT TIMESTAMP
                );''',
    )
    
    
    prepare_insert_data = PostgresOperator(
        task_id='insert_row',
        postgres_conn_id='pg_data',
        sql='''INSERT INTO ORDER_TBL(
                ID,
                STUDENT_ID,
                TEACHER_ID,
                STAGE,
                STATUS,
                CREATED_AT,
                UPDATED_AT
                )
                SELECT I,
                TRUNC(RANDOM()*100),
                TRUNC(RANDOM()*10),
                TRUNC(RANDOM()*3),
                TRUNC(RANDOM()*5),
                NOW(),
                NOW()
                FROM GENERATE_SERIES(1,100, 1) AS I
                WHERE NOT EXISTS (SELECT 1 FROM ORDER_TBL)
                ;''',
    )
    
    prepare_mysql = MySqlOperator(
        task_id='insert_sql',
        sql='''CREATE DATABASE IF NOT EXISTS TEST;
                \
                USE TEST;
                \
                CREATE TABLE IF NOT EXISTS RAW_ORDER (
                ID BIGINT PRIMARY KEY AUTO_INCREMENT,
                ORDER_ID BIGINT,
                STUDENT_ID BIGINT,
                TEACHER_ID BIGINT,
                STAGE VARCHAR(10),
                STATUS VARCHAR(512),
                ROW_HASH BIGINT,
                CREATED_AT TIMESTAMP,
                UPDATED_AT TIMESTAMP
                );
                \
                CREATE TABLE IF NOT EXISTS RAW_ORDER_FINAL (
                ID BIGINT PRIMARY KEY AUTO_INCREMENT,
                ORDER_ID BIGINT,
                STUDENT_ID BIGINT,
                TEACHER_ID BIGINT,
                STAGE VARCHAR(10),
                STATUS VARCHAR(512),
                ROW_HASH BIGINT,
                CREATED_AT TIMESTAMP,
                UPDATED_AT TIMESTAMP
                );''',
        mysql_conn_id='mysql_data',
        autocommit=True
        )
        
    export_to_csv=PythonOperator(
            task_id="export_to_csv",
            python_callable=export_to_csv
        )    
    
    export_csv_to_mysql=PythonOperator(
            task_id="export_csv_to_mysql",
            python_callable=export_csv_to_mysql
        )    
    mysql_remove_dublicates=PythonOperator(
            task_id="mysql_remove_dublicates",
            python_callable=mysql_remove_dublicates
        )    
    
   
    
 
 
prepare_create_table >> prepare_insert_data >> prepare_mysql >> export_to_csv >> export_csv_to_mysql >> mysql_remove_dublicates