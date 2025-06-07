from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
# from airflow.providers.google.cloud.operators.dataform import DataformRunWorkflowInvocationOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator
)
from datetime import datetime

with DAG(
    dag_id="crpto-coin-alert-pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # or use a schedule like '@hourly'
    catchup=False,
) as dag:

    # Task 1: Trigger the Publisher Cloud Function
    trigger_publisher_cloud_function = CloudFunctionInvokeFunctionOperator(
        task_id="publisher_cloud_function",
        project_id="e-object-459802-s8",
        location="us-central1",
        input_data={},  # your payload
        function_id="crypto-coin-publisher",
    )



    # Task 2: Compile the Dataform Workspace
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id='compile_dataform',
        project_id='e-object-459802-s8',
        region='us-central1',
        repository_id='crypto-coin-alert',
        compilation_result={
            'workspace': 'projects/e-object-459802-s8/locations/us-central1/repositories/crypto-coin-alert/workspaces/dev'
        }

    )

    # Task 3: Trigger the Dataform Workflow
    trigger_dataform = DataformCreateWorkflowInvocationOperator(
        task_id='trigger_dataform_workflow',
        project_id='e-object-459802-s8',
        region='us-central1',
        repository_id='crypto-coin-alert',
        workflow_invocation={
            'compilation_result': (
                '{{ task_instance.xcom_pull(task_ids="compile_dataform")["name"] }}'
            ),
        }
    )

    # Task 4: Trigger coin alert cloud function
    trigger_coin_alert_cloud_function = CloudFunctionInvokeFunctionOperator(
        task_id="coin_alert_cloud_function",
        project_id="e-object-459802-s8",
        location="us-central1",
        input_data={},  # your payload
        function_id="crypto-coin-alert",
    )

    trigger_publisher_cloud_function >> compile_dataform >> trigger_dataform >> trigger_coin_alert_cloud_function

