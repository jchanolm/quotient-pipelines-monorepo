from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'farcaster_scraper',
    default_args=default_args,
    description='Run Farcaster scraper to collect data',
    schedule_interval='0 */12 * * *',  # Run every 12 hours
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['farcaster', 'scraper'],
)

run_scraper = KubernetesPodOperator(
    task_id="run_farcaster_scraper",
    name="farcaster-scraper",
    namespace="default",
    image="{{conn.docker_registry.host}}/farcaster-scraper:latest",  # Use your registry
    env_vars={
        "NEYNAR_API_KEY": "{{ var.value.NEYNAR_API_KEY }}",
        "MNEMONIC": "{{ var.value.MNEMONIC }}",
        "NEO4J_URI": "{{ var.value.NEO4J_URI }}",
        "NEO4J_USERNAME": "{{ var.value.NEO4J_USERNAME }}",
        "NEO4J_PASSWORD": "{{ var.value.NEO4J_PASSWORD }}",
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
    },
    dag=dag,
) 