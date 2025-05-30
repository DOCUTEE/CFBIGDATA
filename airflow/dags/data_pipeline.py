from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator


with DAG(
    'data_pipeline',
    default_args={
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
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
        start = DummyOperator(task_id='start')
        init_buckets = BashOperator(
                task_id="init_buckets",
                bash_command="docker exec cf-init python /opt/init/create_bucket.py ",
        )
        create_topics = BashOperator(
                task_id="create_topics",
                bash_command="docker exec cf-broker bash /home/appuser/create_topics.sh ",
        )
        init_clickhouse = BashOperator(
                task_id="init_clickhouse",
                bash_command="docker exec cf-clickhouse bash /home/clickhouse/create_table.sh ",
               

        )
        fake_stream = BashOperator(
                task_id="fake_stream",
                bash_command="docker exec cf-fake-stream python /opt/fake-stream/send_by_time.py ",
        )
        spark_clean = BashOperator(
                task_id="spark_clean",
                bash_command="docker exec cf-spark-clean bash /opt/spark/work-dir/submit_get_and_clean.sh ",
        )
        spark_flatten = BashOperator(
                task_id="spark_flatten",
                bash_command="docker exec cf-spark-flatten bash /opt/spark/work-dir/submit_flatten.sh ",
        )
        spark_cleaned = BashOperator(
                task_id="spark_cleaned",
                bash_command="docker exec cf-spark-cleaned bash /opt/spark/work-dir/submit_save_cleaned_data.sh ",
        )
        spark_transform = BashOperator(
                task_id="spark_transform",
                bash_command="docker exec cf-spark-transform bash /opt/spark/work-dir/submit_transform.sh ",
        )
        spark_clickhouse = BashOperator(
                task_id="spark_clickhouse",
                bash_command="docker exec cf-spark-clickhouse bash /opt/spark/work-dir/submit_spark_to_clickhouse.sh ",
        )
        
        start >> init_buckets >> create_topics >> init_clickhouse
        init_clickhouse >> [spark_clean, spark_flatten, spark_cleaned, fake_stream]
        spark_cleaned >> spark_transform
        spark_transform >> spark_clickhouse