from airflow import DAG
from airflow.operators.python_operators import PythonOperator 
from datetime import timedelta, datetime
from scraping import get_player_info

def insert_gsheet():
    print("data inserted in gsheet")

     


default_args ={
 'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)

}



with DAG(
    dag_id = "scraper_dag",
    schedule_interval = "5 * * * *",
    default_args = default_args,
    start_date=datetime(2021, 1, 1),
    catchup = False
) as fp:
    run_scraper = PythonOperator(
        task_id = "run_scraper",
        python_callable = get_player_info,
        op_kwargs={"offset":60}
    )
    
    push_to_gsheet = PythonOperator(
        task_id = "push_to_gsheet",
        python_callable = insert_gsheet,
    )

    run_scraper >> push_to_gsheet