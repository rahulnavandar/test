from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import storage

from google.cloud.bigquery.job import QueryJobConfig
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
# We need to import the bigquery operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.bash_operator import BashOperator

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
        'dag-name': 'Check_Operator_Example',
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
    description='Check Operator Example'
)

"""
Check query & BigQuery Check operator
-Filter on BODS job + status = NEW
-Filter on start time of the job + reformat to support Airflow macro variables (e.g. current day / yesterday / ...)
-Airflow macro variables : https://airflow.apache.org/macros.html
-Check operator documentation :  http://airflow.apache.org/_api/airflow/contrib/operators/bigquery_check_operator/index.html#airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator

Query example:
SELECT  * FROM `usage-data-reporting.DEV_LDZ_BODS_INFO.BODS_GBQ_TRIGGER`
WHERE SERVICE = "J_MXS_BPdata_to_GBQ_data" AND STATUS_GBQ = "NEW" AND FORMAT_TIMESTAMP("%F", START_TIME ) = "2019-05-09"
"""

sql = """
SELECT count(*) FROM `{}.{}_LDZ_BODS_INFO.BODS_GBQ_TRIGGER`
WHERE SERVICE = "J_MXS_BPdata_to_GBQ_data" AND STATUS_GBQ = "NEW" AND FORMAT_TIMESTAMP("%F", START_TIME ) = "{{ ds }}"
""".format(bqproject, datasetenv)

Check_BODS_Status = BigQueryCheckOperator(
    dag=dag,
    task_id='Check_BODS_ETLStatus',
    sql=sql,
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )


Check_BODS_Status.doc_md = """Update log table after successful ETL run"""

"""
The Update_LogTable operator should be the last one in the DAG
We don't specify a trigger rule so this only runs when everything is successful
Documentation trigger rules : http://airflow.apache.org/concepts.html#trigger-rules
"""

sqlupdatelog = """
UPDATE `{}.{}_LDZ_BODS_INFO.BODS_GBQ_TRIGGER`
SET STATUS_GBQ = "DONE"
WHERE SERVICE = "J_MXS_BPdata_to_GBQ_data" AND STATUS_GBQ = "NEW" AND FORMAT_TIMESTAMP("%F", START_TIME ) = "{{ ds }}"
""".format(bqproject, datasetenv)

Update_LogTable = BigQueryOperator(
    dag=dag,
    task_id='Update_LogTable',
    sql=sqlupdatelog,
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Update_LogTable.doc_md = """Update log table after successful ETL run"""

"""
The Failed operator is an example of a trigger rule that only runs when an operator fails. 
In most flows this will be the check operator if the condition that the BODS ETL is not successful. 
This could be an email operator / slack operator
Documentation trigger rules : http://airflow.apache.org/concepts.html#trigger-rules
Documentation email operator : https://airflow.apache.org/_api/airflow/operators/email_operator/index.html#airflow.operators.email_operator.EmailOperator
Check email example in https://cloud.google.com/composer/docs/how-to/using/writing-dags
Setup email using SendGrid : https://cloud.google.com/composer/docs/how-to/managing/creating#notification
"""

Failed = BashOperator(
    task_id='failed',
    bash_command='date',
    dag=dag,
    trigger_rule="one_failed")

# Define how the different steps in the workflow are executed

Check_BODS_Status >> Update_LogTable
Check_BODS_Status >> Failed




