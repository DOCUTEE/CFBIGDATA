from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import docker
from datetime import timedelta


def exec_cmd_in_container(container_name, command):
        client = docker.from_env()
        container = client.containers.get(container_name)
        exec_result = container.exec_run(command, stream=True)
        
        for line in exec_result.output:
            print(f"[{container_name}] {line.decode().strip()}")

with DAG(
    'data_pipeline_tmp',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
        "start_date": datetime(2024, 1, 1),
    },
    description='A DAG to manage and interact with containers in Docker Compose',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
        create_topics = PythonOperator(
                task_id="create_topics",
                python_callable=lambda: exec_cmd_in_container(
                "cf-broker", "bash /home/appuser/create_topics.sh"
                )
        )
        init_clickhouse = PythonOperator(
                task_id="init_clickhouse",
                python_callable=lambda: exec_cmd_in_container(
                "cf-clickhouse", "bash /home/clickhouse/create_table.sh"
                )
        )
        fake_stream = PythonOperator(
                task_id="fake_stream",
                python_callable=lambda: exec_cmd_in_container(
                "cf-fake-stream", "python /opt/fake-stream/send_by_time.py"
                )
        )
        spark_clean = PythonOperator(
                task_id="spark_clean",
                python_callable=lambda: exec_cmd_in_container(
                "cf-spark-clean", "bash /opt/spark/work-dir/submit_get_and_clean.sh"
                )
        )
        spark_flatten = PythonOperator(
                task_id="spark_flatten",
                python_callable=lambda: exec_cmd_in_container(
                "cf-spark-flatten", "bash /opt/spark/work-dir/submit_flatten.sh"
                )
        )
        create_topics >> init_clickhouse
        init_clickhouse >> [fake_stream, spark_clean, spark_flatten]
        