from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

dag = DAG(
    "emr_cluster_dag",
    start_date=datetime(2021, 7, 27),
    schedule_interval="@daily",
    catchup=False
)

JOB_FLOW_OVERRIDES = {
    "Name": "Raw youtube data processor",
    "ReleaseLabel": "emr-6.3.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master Node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

SPARK_STEPS = [
    {
        "Name": "Process raw channels",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--class",
                "org.ua.wozzya.data.warehouse.formation.ChannelProcessor",
                "s3://chaikovskyi-jars-bucket/spark.jar",
                "--metastore-path",
                "s3n://chaikovskyi-metastore-bucket/channels/",
                "--warehouse-path",
                "s3n://chaikovskyi-data-warehouse-bucket/channels/",
                "--raw-data-path",
                "s3n://chaikovskyi-data-lake-bucket/input/channels",
            ]
        }
    }
]

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag
)

last_step = len(SPARK_STEPS) - 1
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
            + str(last_step)
            + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
