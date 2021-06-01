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
        sql='''CREATE TABLE IF NOT EXISTS order_tbl(
                id bigint primary key,
                student_id bigint,
                teacher_id bigint,
                stage varchar(10),
                status varchar(512),
                created_at timestamp,
                updated_at timestamp
                );''',
    )
    
    
    prepare_insert_data = PostgresOperator(
        task_id='insert_row',
        postgres_conn_id='pg_data',
        sql='''insert into order_tbl(
                id,
                student_id,
                teacher_id,
                stage,
                status,
                created_at,
                updated_at
                )
                select i,
                trunc(random()*100),
                trunc(random()*10),
                trunc(random()*3),
                trunc(random()*5),
                now(),
                now()
                from generate_series(1,100, 1) as i
                WHERE NOT EXISTS (SELECT 1 FROM order_tbl)
                ;''',
    )
    
    prepare_mysql = MySqlOperator(
        task_id='insert_sql',
        sql='''create database if not exists test;
                \
                USE test;
                \
                CREATE TABLE IF NOT EXISTS raw_order (
                id bigint primary key AUTO_INCREMENT,
                order_id bigint,
                student_id bigint,
                teacher_id bigint,
                stage varchar(10),
                status varchar(512),
                row_hash bigint,
                created_at timestamp,
                updated_at timestamp
                );
                \
                CREATE TABLE IF NOT EXISTS raw_order_final (
                id bigint primary key AUTO_INCREMENT,
                order_id bigint,
                student_id bigint,
                teacher_id bigint,
                stage varchar(10),
                status varchar(512),
                row_hash bigint,
                created_at timestamp,
                updated_at timestamp
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