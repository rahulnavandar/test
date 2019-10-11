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
#    "start_date": datetime.today() - timedelta(1),
    "start_date": datetime(2019, 10, 3),
    "email": "business.intelligence@springernature.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'createMD_Usage',
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
    description='Prepare IFM Usage Layer'
)

# define the first task, in our case a big query operator
# Source Tables: Usage_C4_SLINK_BP_Base_Reporting_Monthly, Usage_C4_Magnus_BP_Base_Reporting_Monthly, Usage_C4_Database_BP_Base_Reporting_Monthly, Usage_C4_Nature_BP_Base_Reporting_Monthly
# Target Table: UsageProductMaster_BK
Create_PM_BusinessKeys = BigQueryOperator(
    dag = dag,  # need to tell airflow that this task belongs to the dag we defined above
    task_id='Create_PM_BusinessKeys',  # task id's must be uniqe within the dag
    sql='ProductMaster/05_prepIFM_PM_Usage_BK.sql',  # the actual sql command we want to run on bigquery is in this file in the same folder. it is also templated
    params={"project": bqproject, "environment": datasetenv},  # the sql file above have a template in it with parameters. This is how we pass it in
    write_disposition='WRITE_APPEND',  # Specify the write disposition
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id'] # this is the airflow connection to gcp we defined in the front end.
 )

# add documentation for what this task does - this will be displayed in the Airflow UI
Create_PM_BusinessKeys.doc_md = """Add all keys for all types of usage to BK table"""

# Source Tables:
# Target Table:
Create_MISSING_BK_PMKEY = BigQueryOperator(
    dag = dag,
    task_id='Collect_Missing_PMkeys',
    sql='Usage/02_collect_missing_BK_pmkey.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )
Create_MISSING_BK_PMKEY.doc_md = """Create List of missing Master Data pmkeys"""


#source table:
#target table: Chapter_usage
Create_Chapter_STG_Records = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Chapter_Records',
    sql='ProductMaster/03_chapter_usage_missing.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.chapters_usage',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Chapter_STG_Records.doc_md = """Create Chapter Master Data in Staging"""

#source table:
#target table: Article_usage
Create_Article_STG_Records = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Article_STG_Records',
    sql='ProductMaster/03_article_usage_missing.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.articles_usage',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Article_STG_Records.doc_md = """Create Article Master Data in Staging"""

# Define how the different steps in the workflow are executed
#source table:
#target table: Articles_usage
Create_Article_DWH_Records = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Article_DWH_Records',
    sql='ProductMaster/04_article_usage.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Articles_usage',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Article_DWH_Records.doc_md = """Create Article Master Data in DWH"""

# Define how the different steps in the workflow are executed
#source table:
#target table: Chapters_usage
Create_Chapter_DWH_Records = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Chapter_DWH_Records',
    sql='ProductMaster/04_chapter_usage.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Chapters_usage',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Chapter_DWH_Records.doc_md = """Create Chapter Master Data in DWH"""

# Define how the different steps in the workflow are executed
#source table: Chapters_usage, Chapters
#target table: Chapters_consolidated
Create_Chapter_Consolidated = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Chapter_consolidated',
    sql='ProductMaster/05_chapters_consolidated.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Chapters_consolidated',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Chapter_Consolidated.doc_md = """Consolidate Chapter Master Data in DWH"""

# Define how the different steps in the workflow are executed
#source table: Chapters_usage, Chapters
#target table: Chapters_consolidated
Create_Article_Consolidated = BigQueryOperator(
    dag = dag,
    task_id='Create_Usage_Article_consolidated',
    sql='ProductMaster/05_articles_consolidated.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Articles_consolidated',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

Create_Article_Consolidated.doc_md = """Consolidate Chapter Master Data in DWH"""
# Define how the different steps in the workflow are executed

Create_PM_BusinessKeys >> Create_MISSING_BK_PMKEY >> [ Create_Chapter_STG_Records, Create_Article_STG_Records ]
Create_Article_STG_Records >> Create_Article_DWH_Records >> Create_Article_Consolidated
Create_Chapter_STG_Records >> Create_Chapter_DWH_Records >> Create_Chapter_Consolidated




