"# airflow_task" https://www.notion.so/Data-Engineer-Skyeng-4f6072b9952846708c31c76824f69654

-To run containers use "docker compose up"

-Make settings for database:
	1) Run airflow web interface http://localhost:8080 with credentials airflow\airflow
	
	2) Go to Admin->Connections
	
	3) Add 2 connections:

		mysql_data:
		====

		Conn Id		mysql_data
		Conn Type	MySQL
		Host		mysql_data
		Login		root
		Password 	dataflow	
		Port 		3306


		pg_data:
		====
		Conn Id 		pg_data
		Conn Type 		Postgres
		Host			postgres_data
		Schema			dataflow
		Login			dataflow
		Password		dataflow
		Port			5433


-Go to Dags and start "ETL_task"

-Check Postgres database has table order_tbl with data 

-Check MySQL database has 2 tables  raw_order, raw_order_final

-Check file with data dags\files\export_data.csv
