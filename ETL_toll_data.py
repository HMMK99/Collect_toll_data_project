# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


#defining DAG arguments
default_args ={
    'owner': 'Hatem Kamal',
    'start_date': days_ago(0),
    'email': ['HatemKamal1999@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xvzf tolldata.tgz -C home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/staging/vehicle-data.csv\
                  > /home/project/airflow/dags/staging/csv_data.csv',
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command= 'cut -f5-7 /home/project/airflow/dags/staging/tollplaza-data.tsv\
                  > /home/project/airflow/dags/staging/tsv_data_temp.tsv;\
                  tr "\t" ","< /home/project/airflow/dags/staging/tsv_data_temp.tsv\
                  > /home/project/airflow/dags/staging/tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_fixed_width',
    bash_command = 'tr -s ' ' < /home/project/airflow/dags/staging/payment-data.txt >\
                   /home/project/airflow/dags/staging/one_space_file.txt;\
                   cut " " -f11,12 /home/project/airflow/dags/staging/one_space_file.txt\
                   > /home/project/airflow/dags/staging/fixed_width_data.txt;\
                   tr " " ","< /home/project/airflow/dags/staging/fixed_width_data.txt\
                   > /home/project/airflow/dags/staging/fixed_width_data.csv ',
    dag = dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste /home/project/airflow/dags/staging/csv_data.csv\
                    /home/project/airflow/dags/staging/tsv_data.csv\
                    /home/project/airflow/dags/staging/fixed_width_data.csv\
                    > /home/project/airflow/dags/staging/extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    # transform the 4th column to upper letters
    bash_command = 'awk -F\',\' \'\{print $1","$2","$3","toupper($4)","$5","$6","\
                    $7","$8","$9","$10\}\'< \
                    /home/project/airflow/dags/staging/extracted_data.csv >\
                    /home/project/airflow/dags/staging/transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> \
    extract_data_from_fixed_width >> consolidate_data >> transform_data
