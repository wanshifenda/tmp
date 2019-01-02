from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from datetime import datetime, timedelta
import os
import logging
import random

# one_day_ago = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
default_args = {
    'owner': 'Terence',
    'depends_on_past': False,
    # 'start_date': one_day_ago,
    'retries': 1,
    'email': ['terence.shi@bbc.co.uk'],
    'email_on_failure': False,
    'email_on_retry': False
}

def download_files_from_ftp(remote_path, buffer_root, buffer_folder_prefix, conn_id, ext, **kwargs):
    conn = FTPHook(ftp_conn_id=conn_id)
    date = str(kwargs['execution_date'].day) + '-' + str(kwargs['execution_date'].month) + '-' + str(kwargs['execution_date'].year)
    """
    To make a random buffer folder
    """
    buffer_path = buffer_root + "_".join([buffer_folder_prefix, datetime.now().strftime("%Y%m%d_%H%M%S"), format(random.randint(1, 1000))]) + "/"
    try:
        os.stat(buffer_path)
    except:
        os.makedirs(buffer_path)
    remote_files = []
    total_size = 0
    total_num_of_file = 0
    downloaded_files = []
    remote_files = conn.list_directory(remote_path)
    for remote_file in remote_files:
        fname = os.path.basename(remote_file)
        f,fextension = os.path.splitext(remote_file)
        if fextension != ext:
            logging.info('File extension should be: {}'.format(ext))   
            logging.info('This file is: {}'.format(fextension))   
            logging.info('Skipping file: {}'.format(remote_file))            
        else:
            total_num_of_file = total_num_of_file + 1
            local_filepath = buffer_path + fname
            logging.info('Getting file: {}'.format(remote_file))
            conn.retrieve_file(remote_file, local_filepath)    
            downloaded_files.append(local_filepath)
            logging.info('File has been downloadeded to: {}'.format(local_filepath))
            total_size = total_size + os.path.getsize(local_filepath)
    logging.info('Total size of file transferred: {}' . format(total_size))
    logging.info('Total number of file transferred: {}'. format(total_num_of_file))
    conn.close_conn()
    return downloaded_files

def upload_files_to_gcs(conn_id, bucket, folder_path, **kwargs):
    conn = GoogleCloudStorageHook(google_cloud_storage_conn_id=conn_id)
    files = kwargs['ti'].xcom_pull(task_ids='download_files')
    for file in files:
        fname = os.path.basename(file)
        try:
            os.stat(file)
            if not conn.exists(bucket, folder_path + fname):
                conn.upload(bucket, folder_path + fname, file)
                logging.info('File has been uploaded to bucket: {}' . format(fname))
            else:
                logging.info('Skipping existed file: {}' . format(fname)) 
        except:
            logging.info('Skipping missing local file: {}'.format(file))         
    return files

def delete_tmp(buffer_folder_prefix, **kwargs):
    files = kwargs['ti'].xcom_pull(task_ids='upload_files')
    f,ext = os.path.splitext(files[0])
    for file in files:
        try:
            os.remove(file)
            logging.info('Temp file has been removed: {}' . format(file))
        except OSError, e:
            logging.info('Skipping missing file: {}' . format(file))
    if buffer_folder_prefix in f:
        try:
            os.rmdir(os.path.dirname(files[0]))
            logging.info('Temp folder has been removed: {}' . format(f))
        except OSError, e:
            logging.info('Skipping missing temp folder: {}' . format(f))

dag = DAG(
    'Ingestion_from_FTP',
    default_args = default_args,
    description = 'FTP to Google Cloud Storage',
    schedule_interval = '0 0 * * 0',
    start_date = datetime(2019, 1, 1),
    catchup = False
)

t1 = PythonOperator(
    task_id = 'download_files',
    python_callable = download_files_from_ftp,
    provide_context = True,
    op_kwargs = {
        'remote_path': '/weather/',
        'buffer_root': '/home/airflow/files/',
        'buffer_folder_prefix': 'ftp_task_buffer',
        'conn_id': 'test_ftp',
        'ext': '.txt'
    },
    dag = dag
)

t2 = PythonOperator(
    task_id = 'upload_files',
    python_callable = upload_files_to_gcs,
    provide_context = True,
    op_kwargs = {
        'bucket': 'europe-west1-composer-airfl-c3a79d8c-bucket',
        'conn_id': 'google_cloud_storage_default',
        'folder_path': 'data/'
    },
    dag = dag
)

t3 = PythonOperator(
    task_id = 'delete_buffer_files',
    python_callable = delete_tmp,
    provide_context = True,
    op_kwargs = {
        'buffer_folder_prefix': 'ftp_task_buffer'
    },
    dag = dag
)

t1 >> t2 >> t3
