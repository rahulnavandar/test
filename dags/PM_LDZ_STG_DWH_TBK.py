from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import storage

from google.cloud.bigquery.job import QueryJobConfig
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
# We need to import the bigquery operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "SpringerNature",
    "depends_on_past": False,
    "start_date": datetime.today() - timedelta(1),
    "email": "koen.verschaeren@ml6.eu",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'PM_LDZ_STG_DWH_BKTEST',
        'connection-id': 'springer'
    }
}

# Interval of DAG
schedule_interval = '0 7 * * *'

# Global variables
bqproject = 'usage-data-reporting'
datasetenv = 'DEV'


# Create DAG
dag = DAG(
    ENVIRONMENTS['dev']['dag-name'],
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    description='Move PM data from LDZ/STG to DWH'
)

STG_BK_Articles = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Articles',
    sql='ProductMaster/00_STG_BK_Articles.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_ArticleDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Articles.doc_md = """Upsert Article business keys"""

STG_BK_Articles



