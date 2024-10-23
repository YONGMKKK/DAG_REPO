from airflow import DAG

# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta
import os

working_folder = '/tmp/all_test_files/'
local_configmap_yaml_path = working_folder + 'configmap_hdfs.yaml'
local_k8s_yaml_path = working_folder + 'sparkhdfs.yaml'
spark_files_folder = working_folder + 'all_test_files'
hdfs_data_folder = working_folder + 'all_test_files' + '/test_data'
results_files_folder = working_folder + 'all_test_files' + '/results'

wait_min = 5
ns_name = 'sparkhdfs'
pod_name = 'sparkhdfs-master-namenode-0'
spark_master_container_name = 'bitnami-spark-master'
hdfs_name_container_name = 'hdfs-namenode'

# Default arguments for the DAG
default_args = {
    'owner': 'yong',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1
}

# Define the DAG
with DAG(
    'spark_hdfs_cluster_test_dag_v2',
    default_args = default_args,
    schedule_interval = None,  # Set to None or any cron schedule
    catchup = False
) as dag:
    
    # Task 1: Start the Spark and HDFS clusters
    apply_configmap_cmd = 'kubectl apply -f {}'.format(local_configmap_yaml_path)
    apply_configmap = BashOperator(
        task_id = 'apply_configmap',
        bash_command = apply_configmap_cmd,
        dag = dag
    )

    # Task 1: Start the Spark and HDFS clusters
    start_cluster_cmd = 'kubectl apply -f {}'.format(local_k8s_yaml_path)
    start_cluster = BashOperator(
        task_id = 'start_spark_hdfs_cluster',
        bash_command = start_cluster_cmd,
        dag = dag
    )

    # Task 1.B: Extra - wait some minutes for the cluster starting
    wait_5_minutes = TimeDeltaSensor(
        task_id = 'wait_{}_minutes'.format(wait_min),
        delta = timedelta(minutes = wait_min)
    )

    # Task 2: Copy the test data to the name node
    hdfs_files_cp_cmd = 'kubectl cp {} {}:/tmp/ -c {} -n {}'.format(
        hdfs_data_folder, 
        pod_name, 
        hdfs_name_container_name,
        ns_name
    )    
    copy_files_to_hdfs = BashOperator(
        task_id = 'copy_files_to_container_hdfs_namenode',
        bash_command = hdfs_files_cp_cmd,
        dag = dag
    )

    # Task 3: Put the test data in the HDFS
    put_hdfs_cmd = 'kubectl exec -it {} -c {} -n {} -- /bin/bash -c "{}"'.format(
        pod_name, 
        hdfs_name_container_name, 
        ns_name,
        'hdfs dfs -put -f /tmp/test_data /'
    )
    put_files_to_hdfs = BashOperator(
        task_id = 'put_files_to_hdfs',
        bash_command = put_hdfs_cmd,
        dag = dag
    )

    # Task 4: Copy the test data and .py file into the Spark master container
    spark_files_cp_cmd_1 = 'kubectl cp {}/test_updated.py {}:/tmp/ -c {} -n {}'.format(
        spark_files_folder,
        pod_name,
        spark_master_container_name,
        ns_name
    )
    spark_files_cp_cmd_2 = 'kubectl cp {}/hadoop-2.7.7.tar.gz {}:/tmp/ -c {} -n {}'.format(
        spark_files_folder,
        pod_name,
        spark_master_container_name,
        ns_name
    )
    copy_files_to_spark = BashOperator(
        task_id = 'copy_files_to_container_spark_master',
        bash_command = spark_files_cp_cmd_1 + ' && ' + spark_files_cp_cmd_2,
        dag = dag
    )

    # Task 5: Prepare the environment (use a shell script?)
    spark_prepare_env_cmd_1 = """kubectl exec -it {} -c {} -n {} -- /bin/bash -c "{} && {} && {} && {}" """.format(
        pod_name, 
        spark_master_container_name,
        ns_name, 
        'pip install numpy',
        'pip install prophet',
        'pip install fsspec',
        'pip install pyarrow==7.0.0'
    )
    prepare_spark_env_1 = BashOperator(
        task_id = 'prepare_spark_environment_1',
        bash_command = spark_prepare_env_cmd_1,
        dag = dag
    )

    spark_prepare_env_cmd_2 = """kubectl exec -it {} -c {} -n {} -- /bin/bash -c "{} && {}" """.format(
        pod_name, 
        spark_master_container_name,
        ns_name, 
        'cd /tmp',
        'tar -xvf hadoop-2.7.7.tar.gz'
    )
    prepare_spark_env_2 = BashOperator(
        task_id = 'prepare_spark_environment_2',
        bash_command = spark_prepare_env_cmd_2,
        dag = dag
    )

    # Task 6: Execute commands inside the Kubernetes container
    spark_execute_cmd = 'kubectl exec -it {} -c {} -n {} -- /bin/bash -c "{}"'.format(
        pod_name, 
        spark_master_container_name, 
        ns_name,
        'export HADOOP_HOME="/tmp/hadoop-2.7.7" && export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob` && spark-submit --conf spark.default.parallelism=1 /tmp/test_updated.py',
    )
    spark_submit_job = BashOperator(
        task_id = 'submit_spark_job',
        bash_command = spark_execute_cmd,
        dag = dag
    )
  
    # # Task 7: Download from the HDFS
    # download_models_hdfs_cmd = 'kubectl exec -it {} -c {} -n {} -- /bin/bash -c "{} && {}"'.format(
    #     pod_name, 
    #     hdfs_name_container_name, 
    #     ns_name,
    #     'mkdir /tmp/results',
    #     'hdfs dfs -get /test_data/model/ /tmp/results/',
    # )
    # download_models_hdfs = BashOperator(
    #     task_id = 'get_files_to_hdfs',
    #     bash_command = download_models_hdfs_cmd,
    #     dag = dag
    # )

    # Task 8: Download to the local PC
    download_models_local_pc_cmd = 'kubectl cp {}/{}:/tmp/results/ {} -c {} '.format(
        ns_name,
        pod_name,
        results_files_folder,        
        spark_master_container_name        
    )
    download_models_local_pc = BashOperator(
        task_id = 'download_models_to_local_pc',
        bash_command = download_models_local_pc_cmd,
        dag = dag
    )

    # Task 9: Clean
    cleanup_clusters = BashOperator(
        task_id='cleanup_kubernetes_cluster',
        bash_command='kubectl delete -f {}'.format(local_k8s_yaml_path),
        # bash_command='echo "Clean Up Stage Skipped"',
        dag=dag
    )

    # DAG task dependencies
    # start_cluster >> wait_5_minutes >> copy_files_to_hdfs >> put_files_to_hdfs >> copy_files_to_spark >> prepare_spark_env_1 >> prepare_spark_env_2 \
    # >> spark_submit_job >> download_models_hdfs >> download_models_local_pc >> cleanup_clusters

    start_cluster >> wait_5_minutes >> copy_files_to_hdfs >> put_files_to_hdfs >> copy_files_to_spark >> prepare_spark_env_1 >> prepare_spark_env_2 \
    >> spark_submit_job >> download_models_local_pc >> cleanup_clusters
