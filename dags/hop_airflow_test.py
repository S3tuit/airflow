from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from docker.types import Mount
import os

default_args = {
    'owner' : 'airflow',
    'description' : 'test-pipeline',
    'depend_on_past' : False,
    'start_fate' : datetime(2022, 1, 1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay' : timedelta(seconds=10)
}

@dag(
    schedule_interval = None,
    catchup=False,
    default_args=default_args,
    tags=['hop_airflow_test']
)
def hop_airflow_test():

    @task(task_id = 'start_dag')
    def start_dag():
        docker_host = os.getenv('DOCKER_HOST')
        print(f'DOCKER_HOST: {docker_host}')
        print('Ciao Peppa Pig!')
        
    t1 = BashOperator(
            task_id='docker_context_ls',
            bash_command='docker context ls',
        )
        
    @task(task_id = 'end_dag')
    def end_dag():
        print('Addio Peppa Pig :(')
    
    hop = DockerOperator(
        task_id='sample-pipeline',
        # use the Apache Hop Docker image. Add your tags here in the default apache/hop: syntax
        image='apache/hop:2.7.0',
        api_version='auto',
        container_name = 'hop_2.7',
        auto_remove=True,
        docker_url = 'unix:///var/run/docker.sock',
        environment= {
            'HOP_RUN_PARAMETERS': 'INPUT_DIR=',
            'HOP_LOG_LEVEL': 'Basic',
            'HOP_FILE_PATH': '${PROJECT_HOME}/transforms/repeat_child.hpl',
            'HOP_PROJECT_DIRECTORY': '/project',
            'HOP_PROJECT_NAME': 'hop-airflow-sample',
            'HOP_ENVIRONMENT_NAME': 'env-hop-airflow-sample.json',
            'HOP_ENVIRONMENT_CONFIG_FILE_NAME_PATHS': '/project-config/env-hop-airflow-sample.json',
            'HOP_RUN_CONFIG': 'local'
        },
        network_mode="bridge",
        mounts=[Mount(source=r'C:/Users/matte/Desktop/WORKSPACE/airflow-hop-test/hop_configuration/hop_project', target='/project', type='bind'),
                Mount(source=r'C:/Users/matte/Desktop/WORKSPACE/airflow-hop-test/hop_configuration/hop_env', target='/project-config', type='bind')],
        force_pull=False,
        mount_tmp_dir=False
        )


    dag_start = start_dag()
    dag_end = end_dag()
    
    dag_start >> t1 >> hop >> dag_end

hop_airflow_test()