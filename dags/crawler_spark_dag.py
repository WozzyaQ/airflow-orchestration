import json
from datetime import datetime

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule


def invoke_lambda(**kwargs):
    lambda_client = boto3.client("lambda", region_name=kwargs["function_region"])
    lambda_client.invoke(
        FunctionName=kwargs["function_name"],
        InvocationType="Event",
        Payload=json.dumps(kwargs["json_payload"])
    )


config = Variable.get("config", deserialize_json=True)
channel_videos_payload = config["channel_videos_payload"]
video_comments_payload = config["video_comments_payload"]

with DAG("crawler_spark_scheduler",
         start_date=datetime(2021, 7, 20),
         schedule_interval="@daily",
         catchup=False) as dag:
    JOB_FLOW_OVERRIDES = config["JOB_FLOW_OVERRIDES"]

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ALL_DONE
    )

    PROCESS_CHANNELS_STEPS = config["PROCESS_CHANNELS_STEPS"]

    process_channels = EmrAddStepsOperator(
        task_id="process_channels",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=PROCESS_CHANNELS_STEPS,
    )

    PROCESS_CHANNEL_VIDEOS_STEPS = config["PROCESS_CHANNEL_VIDEOS_STEPS"]

    process_channel_videos = EmrAddStepsOperator(
        task_id="process_channel_videos",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=PROCESS_CHANNEL_VIDEOS_STEPS,
    )

    PROCESS_VIDEO_COMMENTS_STEPS = config["PROCESS_VIDEO_COMMENTS_STEPS"]

    process_video_comments = EmrAddStepsOperator(
        task_id="process_video_comments",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=PROCESS_VIDEO_COMMENTS_STEPS,
    )

    TOP_N_ANALYTICS_STEPS = config["TOP_N_ANALYTICS_STEPS"]

    do_top_N_analytics = EmrAddStepsOperator(
        task_id="top_N_analytics",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=TOP_N_ANALYTICS_STEPS
    )

    watch_end_of_tasks = EmrStepSensor(
        task_id="watch_end_of_tasks",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='top_N_analytics', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    crawl_channel_videos = PythonOperator(
        task_id="crawl_channel_videos",
        python_callable=invoke_lambda,
        op_kwargs=channel_videos_payload
    )

    if_crawl_channel_failed = BranchPythonOperator(
        task_id="crawl_channel_videos_failed",
        python_callable=lambda x: "terminate_emr_cluster",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    channel_videos_sensor = S3KeySensor(
        task_id="wait_for_videos",
        bucket_name=channel_videos_payload["json_payload"]["store_bucket"],
        bucket_key=channel_videos_payload["json_payload"]["store_prefix"] + "/{{ ds }}/_SUCCESS",
        timeout=60 * 15
    )

    crawl_video_comments = PythonOperator(
        task_id="crawl_video_comments",
        python_callable=invoke_lambda,
        op_kwargs=video_comments_payload
    )

    video_comments_sensor = S3KeySensor(
        task_id="wait_for_comments",
        bucket_name=video_comments_payload["json_payload"]["store_bucket"],
        bucket_key=video_comments_payload["json_payload"]["store_prefix"] + "/{{ ds }}/_SUCCESS",
        timeout=60 * 15
    )

    if_video_comments_failed = PythonOperator(
        task_id="crawl_video_comments_failed",
        python_callable=lambda x: "terminate_emr_cluster",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    create_emr_cluster >> process_channels >> do_top_N_analytics
    create_emr_cluster >> crawl_channel_videos >> channel_videos_sensor

    channel_videos_sensor >> [if_crawl_channel_failed, process_channel_videos,
                              crawl_video_comments]

    [process_channel_videos, process_video_comments, process_channels] >> do_top_N_analytics

    if_crawl_channel_failed >> terminate_emr_cluster
    if_video_comments_failed >> terminate_emr_cluster

    crawl_video_comments >> video_comments_sensor >> [process_video_comments, if_video_comments_failed]

    do_top_N_analytics >> watch_end_of_tasks >> terminate_emr_cluster
