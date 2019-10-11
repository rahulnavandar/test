from datetime import date, datetime, timedelta

from google.cloud import bigquery
from google.cloud import storage

from google.cloud.bigquery.job import QueryJobConfig
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import DAG
import os
import ast
import time
from airflow.models import Variable
from airflow.operators import BashOperator

DEFAULT_ARGS = {
    "owner": "SpringerNature",
    "depends_on_past": False,
    "start_date": datetime(2019,8,22,0,0,0),
    #"start_date": datetime.today() - timedelta(1),
    "email": "kamal.mal@springernature.com", #"koen.verschaeren@ml6.eu",
    "email_on_failure": True,
    "email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
}

# Global variables
bqproject = os.environ.get("project")
datasetenv = os.environ.get("environment")

ENVIRONMENTS = {
    'pilot': {
        'dag-name': 'C4_DATABASE_USAGE_DAILY_DWH',
        'connection-id': 'springer',
        'project': bqproject,
        'bucket': 'usagedbraw',
        'dataflow-template': 'gs://sprna-dev-ldz-stg-usage/C4UsageFile_Database'
    }
}

# example {"days": ["01-08-2019","02-08-2019"] } or  { "days": ["daily_process"] }
backlogdays_toprocess = os.environ.get("database_backlogdays_toprocess")
backlogdays = ast.literal_eval(backlogdays_toprocess)


def check_logfile(connection_id, bucket,next_task_id,day_format, **kwargs):
    daywohyphen = day_format.strftime("%Y%m%d")
    file = day[6:10] + "/" + day[3:5] + "/" + "output" + daywohyphen + ".log"
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket)
    stats = storage.Blob(bucket=bucket, name=file).exists(storage_client)
    if stats == False:
        errormsg = "File " + str(day_format) + " not found in slink bucket"
        raise ValueError(errormsg)
    else:
        return next_task_id

def copy_file_to_processing(project, bucket, next_task_id,day_format, **kwargs):
    daywohyphen = day_format.strftime("%Y%m%d")
    file = "output" + daywohyphen + ".log"
    file_path = day[6:10] + "/" + day[3:5] + "/" + "output" + daywohyphen + ".log"

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    source_blob = bucket.blob(file_path)
    processing_path = "processing/"

    new_blob = bucket.copy_blob(source_blob, bucket, processing_path + file)
    # source_blob.delete()
    return next_task_id

def clean_dataflow_dir(project, bucket, next_task_id,day_format, **kwargs):
    daywohyphen = day_format.strftime("%Y%m%d")

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix='dataflow-output/' + daywohyphen + '/')

    for blob in blobs:
        blob.delete()

    return next_task_id

def move_file_to_processed(project, bucket, next_task_id,day_format, **kwargs):
    daywohyphen = day_format.strftime("%Y%m%d")
    file = "output" + daywohyphen + ".log"

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    source_blob = bucket.blob("processing/" + file)
    processed_path = "processed/" + day[6:10] + "/" + day[3:5] + "/"

    new_blob = bucket.copy_blob(source_blob, bucket, processed_path + file)
    source_blob.delete()
    return next_task_id


def delete_converted_files(project, bucket, day_format, **kwargs):
    daywohyphen = day_format.strftime("%Y%m%d")

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix='dataflow-output/' + daywohyphen + '/')

    for blob in blobs:
        blob.delete()


# Task ids and interval of DAG
schedule_interval = '0 6 * * *'

# Create DAG to add all the operators to
dag = DAG(
    ENVIRONMENTS['pilot']['dag-name'],
    default_args=DEFAULT_ARGS,
    schedule_interval=schedule_interval,
    catchup=False,
    description='Load Database Usage data'
)

daystoprocess = backlogdays["days"]
index = 0
delay = 1
for day in daystoprocess:
    if day == 'daily_process':
        day = str(datetime.strftime(date.today() - timedelta(1), '%d-%m-%Y'))

    day_format = datetime.strptime(day, '%d-%m-%Y')
    day_format = day_format.date()
    yearmonth = str(day_format)[0:4] + str(day_format)[5:7]
    date = str(day_format)
    datemonth = date[8:10] + '_' + date[5:7]
    yearmonthsep = str(day_format)[0:4] + '/' + str(day_format)[5:7]

    index = index + 1

    #   before processing any day do selective deletion from target objects(STG/DWH Daily)

    seldel_command = "bq query --nouse_legacy_sql 'delete " + '`' + bqproject + "." + datasetenv + "_STG_Usage.UsageDatabase_C4_" + yearmonth + "`" + " WHERE UsageCalenderDate = \"" + date + "\"'" + " && " + \
                     "bq query --nouse_legacy_sql 'delete " + '`' + bqproject + "." + datasetenv + "_DWH_Usage.Usage_C4_Database_BP_Base_Reporting_Daily` WHERE UsageCalenderDate = \"" + date + "\"'" + " && " + \
                     "bq query --nouse_legacy_sql 'delete " + '`' + bqproject + "." + datasetenv + "_DWH_Usage.Usage_C4_Database_Material_Base_Reporting_Daily` WHERE UsageCalenderDate = \"" + date + "\"'"

    taskid = "Sel_Del_STG_DWH_DAILY" + '_' + datemonth
    Targets_seldeldaily = BashOperator(
        task_id=taskid,
        bash_command=seldel_command,
        dag=dag,
    )


    taskid = "check_logfile" + '_' + datemonth  # str(index)
    nexttaskid = "copy_file_to_processing" + '_' + datemonth

    check_file = BranchPythonOperator(
        retries=0,
        task_id=taskid,
        python_callable=check_logfile,
        provide_context=True,
        op_kwargs={
            "connection_id": ENVIRONMENTS['pilot']['connection-id'],
            "bucket": ENVIRONMENTS['pilot']['bucket'],
            "next_task_id": nexttaskid,
            "day_format": day_format
        },
        dag=dag
    )

    taskid = 'copy_file_to_processing' + '_' + datemonth  # + str(index)
    nexttaskid = 'Clean_Dataflow_Directory' + '_' + datemonth  # + str(index)
    copy_file_to_processing = BranchPythonOperator(
        retries=0,
        task_id=taskid,
        python_callable=copy_file_to_processing,
        provide_context=True,
        op_kwargs={
            "project": ENVIRONMENTS['pilot']['project'],
            "bucket": ENVIRONMENTS['pilot']['bucket'],
            "next_task_id": nexttaskid,
            "day_format": day_format
        },
        dag=dag
    )


    #   Before process dataflow clean dataflow dir for that day
    taskid = 'Clean_Dataflow_Directory' + '_' + datemonth  # + str(index)
    nexttaskid = "dataflow_transform_data" + '_' + datemonth
    Clean_Dataflow_Directory = PythonOperator(
        retries=0,
        task_id=taskid,
        provide_context=True,
        python_callable=clean_dataflow_dir,
        op_kwargs={
            "project": ENVIRONMENTS['pilot']['project'],
            "bucket": ENVIRONMENTS['pilot']['bucket'],
            "next_task_id": nexttaskid,
            "day_format": day_format
        },
        dag=dag,
    )

    #   Dataflow run transformation logic and place transform files into dataflow-output directory
    taskid = "dataflow_transform_data" + '_' + datemonth  # + str(index)
    dataflow_conversion = DataflowTemplateOperator(
        retries=0,
        task_id=taskid,
        template=ENVIRONMENTS['pilot']['dataflow-template'],
        dataflow_default_options={
            'project': ENVIRONMENTS['pilot']['project'],
            'zone': 'europe-west1-d',
            'tempLocation': 'gs://sprna-dev-ldz-stg-usage/temp_database/'
        },
        gcp_conn_id=ENVIRONMENTS['pilot']['connection-id'],
        parameters={
            'input_path': 'gs://' + ENVIRONMENTS['pilot'][
                'bucket'] + '/processing/output' + day_format.strftime("%Y%m%d") + '.log',
            'output_path': 'gs://' + ENVIRONMENTS['pilot'][
                'bucket'] + '/dataflow-output/' + day_format.strftime("%Y%m%d") + '/output' + day_format.strftime(
                "%Y%m%d") + '.log'
        },
        dag=dag
    )

    #   Load the converted logfiles from the dataflow-output directory to STG table
    taskid = "Move_data_to_STG" + '_' + datemonth  # + str(index)
    move_data_to_bigquery_stg = GoogleCloudStorageToBigQueryOperator(
        task_id=taskid,
        bucket=ENVIRONMENTS['pilot']['bucket'],
        source_objects=[(
                'dataflow-output/' + day_format.strftime("%Y%m%d") + '/output' + day_format.strftime(
            "%Y%m%d") + '.log*')],
        destination_project_dataset_table=datasetenv + '_STG_Usage.UsageDatabase_C4_' + day_format.strftime("%Y%m"),
        source_format='NEWLINE_DELIMITED_JSON',
        schema_object='usageschema_database.json',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        google_cloud_storage_conn_id=ENVIRONMENTS['pilot']['connection-id'],
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id'],
        dag=dag
    )

    # Load BP base data into DWH object
    taskid = 'LoadBP_DWH' + '_' + datemonth  # + str(index)
    LoadBP = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_BPReporting.sql',
        params={"project": bqproject, "environment": datasetenv, "yearmonth": yearmonth,
                "date": date},
        allow_large_results=True,
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        destination_dataset_table=bqproject + "." + datasetenv + '_DWH_Usage.Usage_C4_Database_BP_Base_Reporting_Daily',
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    # Load Material base data into DWH object
    taskid = 'LoadMaterial_DWH' + '_' + datemonth  # + str(index)
    LoadMaterial = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_MaterialReporting.sql',
        params={"project": bqproject, "environment": datasetenv, "yearmonth": yearmonth, "date": date},
        allow_large_results=True,
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        destination_dataset_table=bqproject + "." + datasetenv + '_DWH_Usage.Usage_C4_Database_Material_Base_Reporting_Daily',
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    # before processing any day do selective deletion from target objects(BP DWH Monthly)
    taskid = "Sel_Del_DWH_BP_Monthly" + '_' + datemonth
    sel_del_bp_monthly = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_SelDeletion_BP_Monthly.sql',
        params={"project": bqproject, "environment": datasetenv, "yearmonthsep": yearmonthsep},
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    # before processing any day do selective deletion from target objects(BP DWH Monthly)
    taskid = "Sel_Del_DWH_Mat_Monthly" + '_' + datemonth
    sel_del_mat_monthly = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_SelDeletion_Mat_Monthly.sql',
        params={"project": bqproject, "environment": datasetenv, "yearmonthsep": yearmonthsep},
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    #   fill monthly aggregate objects
    taskid = 'RefreshMonthlyAggrBPReporting' + '_' + datemonth  # + str(index)
    RefreshMonthlyAggrBPReporting = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_Refresh_BP_MonthlyAggregate.sql',
        params={"project": bqproject, "environment": datasetenv, "yearmonthsep": yearmonthsep},
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        destination_dataset_table=bqproject + "." + datasetenv + '_DWH_Usage.Usage_C4_Database_BP_Base_Reporting_Monthly',
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    taskid = 'RefreshMonthlyAggrMaterialReporting' + '_' + datemonth  # + str(index)
    RefreshMonthlyAggrMaterialReporting = BigQueryOperator(
        dag=dag,
        task_id=taskid,
        sql='Usage/DatabaseUsage/C4_Database_Refresh_Material_MonthlyAggregate.sql',
        params={"project": bqproject, "environment": datasetenv,"yearmonthsep": yearmonthsep},
        write_disposition='WRITE_APPEND',
        use_legacy_sql=False,
        destination_dataset_table=bqproject + "." + datasetenv + '_DWH_Usage.Usage_C4_Database_Material_Base_Reporting_Monthly',
        bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    )

    #   Move the log file to the processed directory to indicate that it is processed
    taskid = 'move_logfile_to_processed' + '_' + datemonth  # + str(index)
    nexttaskid = 'delete_converted_files' + '_' + datemonth  # + str(index)
    move_file_to_processed = BranchPythonOperator(
        retries=0,
        task_id=taskid,
        python_callable=move_file_to_processed,
        provide_context=True,
        op_kwargs={
            "project": ENVIRONMENTS['pilot']['project'],
            "bucket": ENVIRONMENTS['pilot']['bucket'],
            "next_task_id": nexttaskid,
            "day_format": day_format
        },
        dag=dag
    )

    #   Delete converted log files directory
    taskid = 'delete_converted_files' + '_' + datemonth  # + str(index)
    delete_converted_log_files_directory = PythonOperator(
        retries=0,
        task_id=taskid,
        provide_context=True,
        python_callable=delete_converted_files,
        op_kwargs={
            "project": ENVIRONMENTS['pilot']['project'],
            "bucket": ENVIRONMENTS['pilot']['bucket'],
            "day_format": day_format
        },
        dag=dag,
    )

    # #   Missing master data creation BP/Products
    # taskid = 'MergeBP' + '_' + datemonth  # + str(index)
    # MergeBP = BigQueryOperator(
    #     dag=dag,
    #     task_id=taskid,
    #     sql='Usage/DatabaseUsage/C4_Database_MergeBP.sql',
    #     params={"project": bqproject, "environment": datasetenv, "yearmonth": yearmonth, "date": date},
    #     write_disposition='WRITE_APPEND',
    #     use_legacy_sql=False,
    #     bigquery_conn_id=ENVIRONMENTS['pilot']['connection-id']
    # )

    delay = delay + 180

    Targets_seldeldaily >> check_file >> copy_file_to_processing >> Clean_Dataflow_Directory >> dataflow_conversion >> move_data_to_bigquery_stg >> [
        LoadBP, LoadMaterial]
    LoadMaterial >> sel_del_mat_monthly >> RefreshMonthlyAggrMaterialReporting
    LoadBP >> sel_del_bp_monthly >> RefreshMonthlyAggrBPReporting

    [RefreshMonthlyAggrBPReporting,
     RefreshMonthlyAggrMaterialReporting] >> move_file_to_processed >> delete_converted_log_files_directory