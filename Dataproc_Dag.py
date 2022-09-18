from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator,DataprocCreateClusterOperator,DataprocSubmitJobOperator,DataprocDeleteClusterOperator
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
PROJECT_ID='studious-lore-344410'
REGION='us-west1'
CLUSTER_NAME='test-from-composer'
with DAG('dataproc_job_create_submit_delete', description='Dataproc DAG',
          schedule_interval=None,
          start_date=datetime(2022, 9, 17), catchup=False) as dag:
    CLUSTER_CONFIG = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "n1-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
        },
    }
    create_custom = ClusterGenerator(
        task_id="custom_creation",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        num_workers=2,
        region=REGION,
        storage_bucket='rcodetest',
        # metadata={'PIP_PACKAGES': 'pyyaml requests pandas openpyxl pandas_gbq'},
        init_actions_uris=['gs://rcodetest/requirements.sh'],
        num_masters=1,
        master_machine_type='n1-standard-2',
        master_disk_type='pd-standard',
        master_disk_size=100, 
        worker_machine_type='n1-standard-2',
        worker_disk_type='pd-standard',
        worker_disk_size=100).make()
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=create_custom,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        dag=dag
    )
    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://rcodetest/pandasjob.py"},
    }
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID,dag=dag
    )
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        dag=dag
    )
    dummy=DummyOperator(
        task_id="dummy_end",
        dag=dag
    )


    create_cluster >> pyspark_task >> delete_cluster >> dummy
     
