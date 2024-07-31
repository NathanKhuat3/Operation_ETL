from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
import time
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
import boto3
import pandas as pd
import numpy as np

s3 = boto3.client('s3')
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='demo',
    aws_secret_access_key='demo'
)

# aws_credentials 

def handle_outliers_missing_data(df):
    lower_dur = np.percentile(df['Duration'].dropna(), 25)
    upper_dur = np.percentile(df['Duration'].dropna(), 75)
    df['Duration'] = np.where((df['Duration'] > upper_dur) | (df['Duration'] < lower_dur) | (df['Duration'].isna()), df['Duration'].median(), df['Duration'])

    lower_cost = np.percentile(df['Cost'].dropna(), 25)
    upper_cost = np.percentile(df['Cost'].dropna(), 75)
    df['Cost'] = np.where((df['Cost'] > upper_cost) | (df['Cost'] < lower_cost) | (df['Cost'].isna()), df['Cost'].median(), df['Cost'])


def clean_data(df):
    df.drop_duplicates(inplace=True)
    df['Duration'] = df['Duration'].abs()
    df['Cost'] = df['Cost'].abs()
    handle_outliers_missing_data(df)

    df['Recorded'] = pd.to_datetime(df['Recorded'], format="%Y-%m-%d %H:%M")

def etl_data():
    lst = []
    for obj in s3.Bucket('cleaned-transaction-data').objects.all():
        lst.append(obj.key)

    for obj in s3.Bucket('dirty-transaction-data').objects.all():
        if obj.key not in lst:
            obj_df = pd.read_csv(obj.get()['Body'])
            clean_data(obj_df)
            obj_df.to_csv(f"s3://cleaned-transaction-data/{obj.key}", index=False, storage_options=aws_credentials)

def glue_job_s3_redshift_transfer(job_name, **kwargs):
    session = AwsGenericHook(aws_conn_id='aws_s3_con')
      
    boto3_session = session.get_session(region_name='us-east-2')
    
    client = boto3_session.client('glue')
    client.start_job_run(
        JobName=job_name,
    )

def get_run_id():
    time.sleep(8)
    session = AwsGenericHook(aws_conn_id='aws_s3_con')
    boto3_session = session.get_session(region_name='us-east-2')
    glue_client = boto3_session.client('glue')
    response = glue_client.get_job_runs(JobName="s3-redshift-gluejob")
    job_run_id = response["JobRuns"][0]["Id"]
    return job_run_id 

default_args_1 = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2024, 7, 27),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG('operation_dag',
         default_args=default_args_1,
         schedule_interval = '@monthly',
         catchup=False) as dag:


        extract_clean_load_data = PythonOperator(
        task_id = 'extract_clean_load_data',
        python_callable=etl_data
        )

        glue_job_trigger = PythonOperator(
        task_id='tsk_glue_job_trigger',
        python_callable=glue_job_s3_redshift_transfer,
        op_kwargs={
            'job_name': 's3-redshift-gluejob'
        },
        )

        grab_glue_job_run_id = PythonOperator(
        task_id='tsk_grab_glue_job_run_id',
        python_callable=get_run_id,
        )

        is_glue_job_finish_running = GlueJobSensor(
        task_id="tsk_is_glue_job_finish_running",      
        job_name='s3-redshift-gluejob',
        run_id='{{task_instance.xcom_pull("tsk_grab_glue_job_run_id")}}',
        verbose=True,  
        aws_conn_id='aws_s3_con',
        poke_interval=60,
        timeout=3600,
        )

        extract_clean_load_data >> glue_job_trigger >> grab_glue_job_run_id >> is_glue_job_finish_running