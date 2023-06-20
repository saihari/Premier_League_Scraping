# Step 1: Importing Modules
# To initiate the DAG Object
from airflow import DAG

# Importing datetime and timedelta modules for scheduling the DAGs
from datetime import datetime

# Importing operators
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# Step 2: Creating the environment variables

env_vars = {
    "host": "db-service",
    "database": "football-db",
    "user": "user",
    "password": "password",
    "port": "5432",
}

# Step 3: Creating DAG Object
dag = DAG(
    dag_id="PremierLeagueStatsAndRatings",
    start_date=datetime(2023, 6, 19),
    schedule_interval="@once",
)

# Step 4: Creating task
# Creating first task
start = KubernetesPodOperator(
    task_id="ScrapeData",
    name="football-etl-job-scrape-data",
    namespace="default",
    image="football_viz",
    image_pull_policy="IfNotPresent",
    env_vars=env_vars,
    cmds=["python", "job.py"],
    dag=dag,
)

# Creating second task
end = KubernetesPodOperator(
    task_id="CalculateRatings",
    name="football-etl-job-calculate-ratings",
    namespace="default",
    image="football_viz",
    image_pull_policy="IfNotPresent",
    env_vars=env_vars,
    cmds=["python", "ratings.py"],
    dag=dag,
)

# Step 5: Setting up dependencies
start >> end
