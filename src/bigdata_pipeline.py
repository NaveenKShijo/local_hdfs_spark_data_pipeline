from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'Naveen',
    'retries': 3,
    'retry_delay': timedelta(seconds = 2),
    'start_date': datetime(2026, 4, 20)
}

def check_hdfs_has_files():
    result = subprocess.run(
        ['hdfs', 'dfs', '-ls', '/data_lake/bronze/unprocessed/'],
        capture_output=True, text=True
    )
    lines = [l for l in result.stdout.strip().split('\n') if l.startswith('-')]
    return len(lines) > 0

with DAG(
    dag_id = 'hdfs_trigger_pipeline',
    default_args = default_args,
    schedule = "0 */12 * * *",
    catchup = False
) as dag:
    
    wait_for_file = PythonSensor(
        task_id="wait_for_hdfs_file",
        python_callable=check_hdfs_has_files,
        poke_interval=30,
        timeout=600
    )

    bronze_to_silver = BashOperator(
        task_id = 'bronze_to_silver',
        bash_command = '/home/naveen/spark_workout/pyspark_env/bin/spark-submit /home/naveen/spark_workout/project/bronze_to_silver.py',
        env={
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/home/naveen/spark_workout/pyspark_env/bin:/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/bin:/bin',
            'SPARK_HOME': '/home/naveen/spark_workout/pyspark_env/lib/python3.12/site-packages/pyspark'
        }
    )

    move_processed_files = BashOperator(
        task_id = 'move_raw_to_bronze_processed',
        bash_command = """
            hdfs dfs -mv /data_lake/bronze/unprocessed/*.csv \
            /data_lake/bronze/processed/
        """
    )

    silver_to_gold = BashOperator(
        task_id = 'silver_to_gold',
        bash_command = '/home/naveen/spark_workout/pyspark_env/bin/spark-submit /home/naveen/spark_workout/project/silver_to_gold.py',
         env={
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/home/naveen/spark_workout/pyspark_env/bin:/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/bin:/bin',
            'SPARK_HOME': '/home/naveen/spark_workout/pyspark_env/lib/python3.12/site-packages/pyspark'
        }
    )

    wait_for_file >> bronze_to_silver >> move_processed_files >> silver_to_gold 
