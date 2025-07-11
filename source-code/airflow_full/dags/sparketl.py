from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Long',                                  # Chủ sở hữu DAG
    'start_date': datetime(2025, 1, 1),              # Ngày bắt đầu chạy DAG
    'retries': 0,                                    # Số lần retry khi fail
    'retry_delay': timedelta(minutes=2),             # Thời gian chờ giữa các lần retry
    'email': ['nguyenphilong949@gmail.com'],             # Email cảnh báo
    'email_on_failure': True,                        # Gửi email khi task fail
    'email_on_retry': False,                         # Không cần gửi email khi retry
    'depends_on_past': False,                        # Không phụ thuộc vào DAG run trước đó
    'execution_timeout': timedelta(hours=5),         # Timeout cho task (giới hạn treo)
}

with DAG(
    dag_id='sparketl',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='ETL: Raw -> Bronze -> Silver using Spark',
) as dag:

    raw_to_bronze = BashOperator(
        task_id='run_raw_to_bronze',
        bash_command='docker exec spark-master spark-submit /opt/spark/raw_to_bronze.py'
    )

    bronze_to_silver = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='docker exec spark-master spark-submit /opt/spark/bronze_to_silver.py'
    )

    silver_to_gold1 = BashOperator(
        task_id='run_GL_HospitalAdmissionProcess',
        bash_command='docker exec spark-master spark-submit /opt/spark/GL_HospitalAdmissionProcess.py'
    )

    silver_to_gold2 = BashOperator(
        task_id='run_GL_IcuStayMonitoring',
        bash_command='docker exec spark-master spark-submit /opt/spark/GL_IcuStayMonitoring.py'
    )

    silver_to_gold3 = BashOperator(
        task_id='run_GL_DiagnosisProcedureAnalytics',
        bash_command='docker exec spark-master spark-submit /opt/spark/GL_DiagnosisProcedureAnalytics.py'
    )

    hive_metastore = BashOperator(
        task_id='run_hive_metastore',
        bash_command='docker exec spark-master spark-submit /opt/spark/crtmetastore.py'
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold1 >> silver_to_gold2 >> silver_to_gold3 >> hive_metastore
