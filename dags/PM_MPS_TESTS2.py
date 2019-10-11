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
        'dag-name': 'PM_MPS_TESTS2',
        'connection-id': 'springer'
    }
}

# Interval of DAG
schedule_interval = '0 8 * * *'

# Global variables
bqproject = 'usage-data-reporting'
datasetenv = 'DEV'


# Create DAG
dag = DAG(
    ENVIRONMENTS['dev']['dag-name'],
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    description='Move PRPS data from LDZ to STG'
)

DWH_AppendPRPS = BigQueryOperator(
    dag = dag,
    task_id='00_DWH_PRPS',
    sql='ProductMaster/00_STG_DWH_PRPS.sql',
    params={"project": bqproject, "environment": datasetenv},
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.PSP',
    write_disposition='WRITE_TRUNCATE',  # Specify the write disposition
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendPRPS.doc_md = """Write PRPS to DWH"""

# Define how the different steps in the workflow are executed

DWH_AppendPRPS


