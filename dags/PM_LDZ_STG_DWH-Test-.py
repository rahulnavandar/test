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
    "start_date": datetime(2019, 6, 27),
    "email": "business.intelligence@springernature.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'PM_LDZ_STG_DWH-Test-',
        'connection-id': 'springer'
    }
}

# Interval of DAG
schedule_interval = '0 5 * * *'

# Global variables
bqproject = 'usage-data-reporting'
datasetenv = 'DEV'


# Create DAG
dag = DAG(
    ENVIRONMENTS['dev']['dag-name'],
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    description='Move ProductMasterDate from LDZ/STG to DWH --TEST--'
)

STG_BK_Submissions = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Submissions',
    sql='ProductMaster/00_STG_BK_Submissions.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_SubmissionKey',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Submissions.doc_md = """Get all Submission business keys"""

STG_DWH_Submissions = BigQueryOperator(
    dag = dag,
    task_id='01_STG_DWH_Submissions',
    sql='ProductMaster/01_STG_DWH_Submissions.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Submissions',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Submissions.doc_md = """Submissions to DWH"""

DWH_AppendHistSubmissions = BigQueryOperator(
     dag = dag,
     task_id='DWH_AppendHistSubmissions',
     sql='ProductMaster/04_DWH_AppendHistSubmissions.sql',
     params={"project": bqproject, "environment": datasetenv},
     write_disposition='WRITE_APPEND',  # Specify the write disposition
#     destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Submissions_Hist',
     use_legacy_sql=False,
     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
  )

DWH_AppendHistSubmissions.doc_md = """Submissions to DWH"""

# Define how the different steps in the workflow are executed
STG_BK_Submissions >> STG_DWH_Submissions >> DWH_AppendHistSubmissions

