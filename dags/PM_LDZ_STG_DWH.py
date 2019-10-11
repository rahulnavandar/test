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
        'dag-name': 'PM_LDZ_STG_DWH',
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

STG_BK_Articles.doc_md = """Get all Article business keys"""

STG_BK_Books = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Books',
    sql='ProductMaster/00_STG_BK_Books.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_BookDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Books.doc_md = """Get all Book business keys"""

STG_BK_Chapters = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Chapters',
    sql='ProductMaster/00_STG_BK_Chapters.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_ChapterDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Chapters.doc_md = """Get all Chapter business keys"""

STG_BK_Issues = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Issues',
    sql='ProductMaster/00_STG_BK_Issues.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_IssueDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Issues.doc_md = """Geta all Issue business keys"""

STG_BK_Journals = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Journals',
    sql='ProductMaster/00_STG_BK_Journals.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_JournalDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_BK_Journals.doc_md = """Get all Journal business keys"""

STG_DWH_Articles = BigQueryOperator(
    dag = dag,
    task_id='01_STG_DWH_Articles',
    sql='ProductMaster/01_STG_DWH_Articles.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Articles',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Articles.doc_md = """Articles to DWH"""

STG_DWH_Chapters = BigQueryOperator(
    dag = dag,
    task_id='01_STG_DWH_Chapters',
    sql='ProductMaster/01_STG_DWH_Chapters.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Chapters',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Chapters.doc_md = """Chapters to DWH"""

STG_DWH_Books = BigQueryOperator(
    dag = dag,
    task_id='02_STG_DWH_Books',
    sql='ProductMaster/02_STG_DWH_Books.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Books',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Books.doc_md = """Books to DWH"""

STG_DWH_Issues = BigQueryOperator(
    dag = dag,
    task_id='02_STG_DWH_Issues',
    sql='ProductMaster/02_STG_DWH_Issues.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',  # Specify the write disposition
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Issues',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Issues.doc_md = """Issues to DW"""

STG_DWH_Journals = BigQueryOperator(
    dag = dag,
    task_id='03_STG_DWH_Journals',
    sql='ProductMaster/03_STG_DWH_Journals.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',  # Specify the write disposition
    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Journals',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

STG_DWH_Journals.doc_md = """Journals to DWH"""

DWH_AppendHistArticles = BigQueryOperator(
    dag = dag,
    task_id='DWH_AppendHistArticles',
    sql='ProductMaster/04_DWH_AppendHistArticles.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
#    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Articles_Hist',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendHistArticles.doc_md = """Journals to DWH"""

DWH_AppendHistIssues = BigQueryOperator(
    dag = dag,
    task_id='DWH_AppendHistIssues',
    sql='ProductMaster/04_DWH_AppendHistIssues.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
#    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Issues_Hist',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendHistIssues.doc_md = """Journals to DWH"""

DWH_AppendHistJournals = BigQueryOperator(
    dag = dag,
    task_id='DWH_AppendHistJournals',
    sql='ProductMaster/03_STG_DWH_Journals.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
#    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Journals_Hist',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendHistJournals.doc_md = """Journals to DWH"""

DWH_AppendHistChapters = BigQueryOperator(
    dag = dag,
    task_id='DWH_AppendHistChapters',
    sql='ProductMaster/04_DWH_AppendHistChapters.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
#    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Chapters_Hist',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendHistChapters.doc_md = """Journals to DWH"""

DWH_AppendHistBooks = BigQueryOperator(
    dag = dag,
    task_id='DWH_AppendHistBooks',
    sql='ProductMaster/04_DWH_AppendHistBooks.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_APPEND',  # Specify the write disposition
#    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Books_Hist',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
 )

DWH_AppendHistBooks.doc_md = """Journals to DWH"""

# Define how the different steps in the workflow are executed
[STG_BK_Chapters, STG_BK_Books] >> STG_DWH_Chapters >> STG_DWH_Books >> [DWH_AppendHistChapters, DWH_AppendHistBooks]
[STG_BK_Articles, STG_BK_Issues, STG_BK_Journals] >> STG_DWH_Articles >> STG_DWH_Issues >> STG_DWH_Journals >> [DWH_AppendHistArticles, DWH_AppendHistIssues, DWH_AppendHistJournals]

