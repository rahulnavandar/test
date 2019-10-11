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
    "start_date": datetime(2019, 8, 7, 1, 0, 0, 0),
    "email": "koen.verschaeren@ml6.eu",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'MD_BusinessPartner',
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
    description='Move BP data from LDZ to DWH'
)

# define the first task, in our case a big query operator
bq_task_1 = BigQueryOperator(
    dag = dag,  # need to tell airflow that this task belongs to the dag we defined above
    task_id='crm_addresses',  # task id's must be uniqe within the dag
    sql='BP/BP_LDZ_STG_crm_addresses.sql',  # the actual sql command we want to run on bigquery is in this file in the same folder. it is also templated
    params={"project": bqproject, "environment": datasetenv},  # the sql file above have a template in it with parameters. This is how we pass it in
    #destination_dataset_table=ENVIRONMENTS['dev']['project']+'.'+ ENVIRONMENTS['dev']['dataset'] +'.' +'crm_addresses', # No table needed for DML statements
    write_disposition='WRITE_APPEND',  # Specify the write disposition
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id'] # this is the airflow connection to gcp we defined in the front end.
 )

# add documentation for what this task does - this will be displayed in the Airflow UI
bq_task_1.doc_md = """Load crm_addresses from LDZ to STG"""

bq_task_2 = BigQueryOperator(
    dag = dag,
    task_id='crm_but000',
    sql='BP/BP_LDZ_STG_crm_but000.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_2.doc_md = """Load crm_but000' from LDZ to STG"""

bq_task_3 = BigQueryOperator(
    dag = dag,
    task_id='crm_but0id',
    sql='BP/BP_LDZ_STG_crm_but0id.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_3.doc_md = """Load crm_but0id from LDZ to STG"""

bq_task_4 = BigQueryOperator(
    dag = dag,
    task_id='crm_but050',
    sql='BP/BP_LDZ_STG_crm_but050.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_4.doc_md = """Load crm_but050 from LDZ to STG"""

bq_task_5 = BigQueryOperator(
    dag = dag,
    task_id='erp_addresses',
    sql='BP/BP_LDZ_STG_erp_addresses.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_5.doc_md = """Load erp_addresses from LDZ to STG"""

bq_task_6 = BigQueryOperator(
    dag = dag,
    task_id='erp_but000',
    sql='BP/BP_LDZ_STG_erp_but000.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_6.doc_md = """Load erp_but000 from LDZ to STG"""

bq_task_7 = BigQueryOperator(
    dag = dag,
    task_id='erp_but0id',
    sql='BP/BP_LDZ_STG_erp_but0id.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_7.doc_md = """Load erp_but0id from LDZ to STG"""

bq_task_8 = BigQueryOperator(
    dag = dag,
    task_id='erp_but050',
    sql='BP/BP_LDZ_STG_erp_but050.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_8.doc_md = """Load erp_but050 from LDZ to STG"""

bq_task_9 = BigQueryOperator(
    dag = dag,
    task_id='erp_kna1',
    sql='BP/BP_LDZ_STG_erp_kna1.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_9.doc_md = """Load erp_kna1 from LDZ to STG"""

bq_task_10 = BigQueryOperator(
    dag = dag,
    task_id='erp_zbutmetapress',
    sql='BP/BP_LDZ_STG_erp_zbutmetapress.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

bq_task_10.doc_md = """Load erp_zbutmetapress from LDZ to STG"""

BP_BusinessKeys = BigQueryOperator(
    dag = dag,
    task_id='00_BP_STG_BP_BusinessKeys',
    sql='BP/00_BP_STG_BP_BusinessKeys.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_BusinessPartner.BK_BusinessPartner',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

BP_BusinessKeys.doc_md = """Insert BP business keys"""

BuildBPDimension = BigQueryOperator(
    dag = dag,
    task_id='01_BP_STG_BuildBPDimension',
    sql='BP/01_BP_STG_BuildBPDimension.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_BusinessPartner.BusinessPartner_Step1',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

BuildBPDimension.doc_md = """Build BP Dimension"""

ReplaceBPDimension = BigQueryOperator(
    dag = dag,
    task_id='DWH_ReplaceBPDimension',
    sql='BP/04_BP_STG_ DWH_ReplaceBPDimension.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_BusinessPartner.BusinessPartner',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

ReplaceBPDimension.doc_md = """Move BP dimension to DWH"""

ConsolidateBPDimension = BigQueryOperator(
    dag = dag,
    task_id='DWH_ConsolidateBPDimension',
    sql='BusinessPartner/04_DWH_BP_consolidated.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_BusinessPartner.BusinessPartner_consolidated',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

ConsolidateBPDimension.doc_md = """Consolidate BP"""

# Define how the different steps in the workflow are executed

bq_task_1 >> bq_task_2 >> bq_task_3 >> bq_task_4
bq_task_5 >> bq_task_6 >> bq_task_7 >> bq_task_8 >> bq_task_9 >> bq_task_10

BP_BusinessKeys.set_upstream([bq_task_4, bq_task_10])

BP_BusinessKeys >> BuildBPDimension >> ReplaceBPDimension >> ConsolidateBPDimension