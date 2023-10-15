from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
import datetime
import json
import time

DAG_NAME = 'grushevskiy_fb_processing'
GP_CONN_ID = 'grushevskiy'

args = {'owner': 'grushevskiy',
        'start_date': datetime(2023, 10, 15),
        'retries': 1,
        'retry_delay': timedelta(hours=24)}

SQL_QUERY = "delete from fb_events where data < (current_timestamp::date- interval '1 year');"

with DAG(DAG_NAME,
        description='processing from hdfs to GP and remove',
        schedule_interval='* * * * *',
        catchup=False,
        max_active_runs=1,
        default_args=args,
        params={'labels': {'env': 'prod', 'priority': 'high'}}
        ) as dag:

    hdfs_to_GP = SSHOperator(task_id='get_from_hdfs',
                                        ssh_conn_id='grushevskiy_ssh_vm-cli2_conn',
                                        command='source venv/bin/activate; python3 python/hdfs_to_GP.py'
                                        )
    remove = PostgresOperator(task_id='remove',
                                        sql=SQL_QUERY,
                                        postgres_conn_id=GP_CONN_ID,
                                        autocommit=True)
   hdfs_to_GP >> remove

