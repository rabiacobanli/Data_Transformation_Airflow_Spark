from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook



start_date = datetime(2024, 3, 21)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

endpoint_url = Variable.get("endpoint_url")
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")

ssh_hook = SSHHook(ssh_conn_id="spark_ssh_conn", cmd_timeout=None)

with DAG('final_project', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t0 = DummyOperator(task_id='start_task')

    t1 = SSHOperator(task_id='credits_to_s3',
                    command=f"""cd /home/ssh_train/ && 
                    source datagen/bin/activate && 
                    python /home/ssh_train/data-generator/dataframe_to_s3.py -buc tmdb-bronze -k credits/credits_part -aki {aws_access_key_id} -sac {aws_secret_access_key} -eu {endpoint_url} -i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv -ofp True -z 500 -b 0.1""",
                    execution_timeout=None,
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)
    
    t2 = SSHOperator(task_id='movies_to_s3',
                    command=f"""cd /home/ssh_train/ && 
                    source datagen/bin/activate && 
                    python /home/ssh_train/data-generator/dataframe_to_s3.py -buc tmdb-bronze -k   movies/movies_part -aki {aws_access_key_id} -sac {aws_secret_access_key} -eu {endpoint_url} -i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv -ofp True -z 500 -b 0.1""",
                    execution_timeout=None,
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)
    
    t3 = DummyOperator(task_id='created_data')


    t4 = SSHOperator(task_id='s3_to_spark_to_s3',
                    command=f"""source /dataops/airflowenv/bin/activate &&
                    python /s3_to_spark_to_s3.py """,
                    ssh_conn_id='spark_ssh_conn',
                    ssh_hook=ssh_hook)
                    
    t5 = DummyOperator(task_id='final_task', trigger_rule='none_failed_or_skipped')


    t0 >> [t1 , t2] >> t3 >> t4 >>t5 