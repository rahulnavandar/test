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
    "start_date": datetime(2019, 7, 4),
    "email": "business.intelligence@springernature.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ENVIRONMENTS = {
    'dev': {
        'dag-name': 'MD_ProductMasterData',
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
    description='Load Product Master Data from LDZ to STG/DWH'
)

#STG_BK_Submissions = BigQueryOperator(
#    dag = dag,
#    task_id='00_STG_BK_Submissions',
#    sql='ProductMaster/00_STG_BK_Submissions.sql',
#    params={"project": bqproject, "environment": datasetenv},
#    write_disposition='WRITE_TRUNCATE',
#    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_SubmissionKey',
#    use_legacy_sql=False,
#    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
# )
#
#STG_BK_Submissions.doc_md = """Get all Submission business keys"""

STG_BK_Articles = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Articles',
    sql='ProductMaster/00_STG_BK_Articles.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_ArticleDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
# Generate Article business keys from LDZ/STG to STG area.
# Sources: DDS_ARTICLE_FULL, PM_USAGEARTICLE
# Target: BK_ArticleDOI
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
# Generate Book business keys from LDZ to STG area.
# Sources: BFLUX_BOOKS_DIFF, BFLUX_BOOKS_FULL
# Target: BK_BookDOI
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
# Generate Chapter business keys from LDZ/STG to STG area.
# Sources: DDS_CHAPTERS_FULL, DDS_CHAPTERS_DIFF, PM_USAGECHAPTERBOOK
# Target: BK_ChapterDOI
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
# Generate Issue business keys from LDZ to STG area.
# Sources: JFW_ISSUES, JWF_ISSUE_DATA, JWF_ISSUE_STATS, JFLUX_ISSUE, DDS_ISSUES
# Target: BK_IssueDOI
 )

STG_BK_Issues.doc_md = """Get all Issue business keys"""

STG_BK_Issues_Key_Figures = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Issues_Key_Figures',
    sql='ProductMaster/00_STG_BK_Issues_Key_Figures.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_IssuesKeyFigures',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
# Generate some key figures per issue LDZ to STG area.
# Sources: JFLUX_ISSUE, JFW_ISSUES, JWF_ISSUE_DATA
# Target: BK_IssuesKeyFigures
 )

STG_BK_Issues_Key_Figures.doc_md = """Get all Issue key figures keys"""

STG_BK_Journals = BigQueryOperator(
    dag = dag,
    task_id='00_STG_BK_Journals',
    sql='ProductMaster/00_STG_BK_Journals.sql',
    params={"project": bqproject, "environment": datasetenv},
    write_disposition='WRITE_TRUNCATE',
    destination_dataset_table=bqproject + '.' + datasetenv + '_STG_ProductMaster.BK_JournalDOI',
    use_legacy_sql=False,
    bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
# Generate Journal  business keys from LDZ to STG area.
# Sources: JFW_JOURNALS, JFW_NON_REGULAR_JOURNALS
# Target: BK_JournalDOI
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
# Gather article master data from LDZ to DWH area.
# Sources: DDS_ARTICLE_FULL, DDS_ARTICLE_DIFF, JWF_ARTICLE, JWF_ARTICLE_OFFPRINTS, JFLUX_ARTICLE
# Target: ARTICLES
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
# Gather chapter master data from LDZ to DWH area.
# Sources: BFLUX_CHAPTERS, DDS_CHAPTERS_FULL, DDS_CHAPTERS_DIFF
# Target: CHAPTERS
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
# Gather book master data from LDZ to DWH area.
# Sources: BFLUX_BOOKS_FULL, BFLUX_BOOKS_DIFF, BFLUX_BOOK_PRICES
# Target: BOOKS
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
# Gather issue master data from LDZ to DWH area.
# Sources: DDS_ISSUES, JFLUX_ISSUE, JFW_ISSUES, JWF_ISSUE_DATA, JWF_ISSUE_STATS
# Target: ISSUES
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
# Gather journal master data from LDZ to DWH area.
# Sources: JFW_JOURNALS, JFW_NON_REGULAR_JOURNALS, JFW_PRICES_CURRENT_YEAR, JWF_JOURNAL
# Target: JOURNALS
 )

STG_DWH_Journals.doc_md = """Journals to DWH"""

# DWH_AppendHistArticles = BigQueryOperator(
#     dag = dag,
#     task_id='DWH_AppendHistArticles',
#     sql='ProductMaster/04_DWH_AppendHistArticles.sql',
#     params={"project": bqproject, "environment": datasetenv},
#     write_disposition='WRITE_APPEND',  # Specify the write disposition
# #    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Articles_Hist',
#     use_legacy_sql=False,
#     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
#  )
#
# DWH_AppendHistArticles.doc_md = """Journals to DWH"""
#
# DWH_AppendHistIssues = BigQueryOperator(
#     dag = dag,
#     task_id='DWH_AppendHistIssues',
#     sql='ProductMaster/04_DWH_AppendHistIssues.sql',
#     params={"project": bqproject, "environment": datasetenv},
#     write_disposition='WRITE_APPEND',  # Specify the write disposition
# #    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Issues_Hist',
#     use_legacy_sql=False,
#     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
#  )
#
# DWH_AppendHistIssues.doc_md = """Journals to DWH"""
#
# DWH_AppendHistJournals = BigQueryOperator(
#     dag = dag,
#     task_id='DWH_AppendHistJournals',
#     sql='ProductMaster/03_STG_DWH_Journals.sql',
#     params={"project": bqproject, "environment": datasetenv},
#     write_disposition='WRITE_APPEND',  # Specify the write disposition
# #    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Journals_Hist',
#     use_legacy_sql=False,
#     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
#  )
#
# DWH_AppendHistJournals.doc_md = """Journals to DWH"""
#
# DWH_AppendHistChapters = BigQueryOperator(
#     dag = dag,
#     task_id='DWH_AppendHistChapters',
#     sql='ProductMaster/04_DWH_AppendHistChapters.sql',
#     params={"project": bqproject, "environment": datasetenv},
#     write_disposition='WRITE_APPEND',  # Specify the write disposition
# #    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Chapters_Hist',
#     use_legacy_sql=False,
#     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
#  )
#
# DWH_AppendHistChapters.doc_md = """Journals to DWH"""
#
# DWH_AppendHistBooks = BigQueryOperator(
#     dag = dag,
#     task_id='DWH_AppendHistBooks',
#     sql='ProductMaster/04_DWH_AppendHistBooks.sql',
#     params={"project": bqproject, "environment": datasetenv},
#     write_disposition='WRITE_APPEND',  # Specify the write disposition
# #    destination_dataset_table=bqproject + '.' + datasetenv + '_DWH_ProductMaster.Books_Hist',
#     use_legacy_sql=False,
#     bigquery_conn_id=ENVIRONMENTS['dev']['connection-id']
#  )
#
# DWH_AppendHistBooks.doc_md = """Journals to DWH"""

# Define how the different steps in the workflow are executed
#[STG_BK_Chapters, STG_BK_Books] >> STG_DWH_Chapters >> STG_DWH_Books #>> [DWH_AppendHistChapters, DWH_AppendHistBooks]
#[STG_BK_Articles, STG_BK_Issues, STG_BK_Issues_Key_Figures, STG_BK_Journals] >> STG_DWH_Articles >> STG_DWH_Issues >> STG_DWH_Journals #>> [DWH_AppendHistArticles, DWH_AppendHistIssues, DWH_AppendHistJournals]

STG_BK_Chapters >> STG_DWH_Chapters
STG_BK_Books >> STG_DWH_Books
STG_BK_Articles >> STG_DWH_Articles
[STG_BK_Issues, STG_BK_Issues_Key_Figures] >> STG_DWH_Issues
STG_BK_Journals >> STG_DWH_Journals



