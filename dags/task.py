from datetime import datetime, timedelta
from airflow import DAG
import time
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

base_file_path = "dags/files/"
dag_id = "db_test"

dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2021, 5, 28),
    'schedule_interval': None
}

def extract(**kwargs):
    ti = kwargs['ti']
    # Get the hook
    pgsqlserver = PostgresHook("pg_data")
    connection = pgsqlserver.get_conn()
    cursor = connection.cursor()
    # Execute the query
    cursor.execute("select id, student_id, teacher_id, stage, status, created_at, updated_at from order_tbl")
    sources = cursor.fetchall()
    mysql = MySqlHook('mysql_data')
    mysql.run('USE test')
    mysql.insert_rows(table='test.raw_order', rows=sources)
    print("Export done")
    #return sources

    # Generate somewhat unique filename
    #path = "{}{}_{}.ftr".format(base_file_path, dag_id, int(time.time()))
    # Save as a binary feather file
    #df.to_feather(path)
    #print("Export done")

def save_to_mysql(self, context):
    hook = MySqlHook(mysql_conn_id='mysql_data')
    hook.run(
       '''insert into raw_order (
                id,
                student_id,
                teacher_id,
                stage,
                status 
                ) values''',
        autocommit=self.autocommit,
        parameters=self.parameters)

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
                id bigint primary key,
                student_id bigint,
                teacher_id bigint,
                stage varchar(10),
                status varchar(512),
                created_at timestamp,
                updated_at timestamp
                );''',
        mysql_conn_id='mysql_data',
        autocommit=True
        )
    
    extract_data=PythonOperator(
            task_id="extract",
            python_callable=extract
        )    
    
   
    
 
 
prepare_create_table >> prepare_insert_data >> prepare_mysql >> extract_data