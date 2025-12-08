from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="revenue_iceberg_daily",
    description="Daily revenue Gold pipeline (Iceberg)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["revenue", "gold", "iceberg", "kimball"],
) as dag:

    run_revenue_job = SSHOperator(
        task_id="run_revenue_iceberg_job",
        ssh_conn_id="spark_remote_host",
        command=(
            "spark-submit "
            "{{ var.value.DATA_JOBS_HOME }}/jobs/revenue_iceberg_job.py "
            "{{ var.value.DATA_JOBS_HOME }}/conf/revenue_job.yaml "
            "{{ ds }}"
        ),
    )

    run_revenue_job