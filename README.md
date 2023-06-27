# Premier League Stats

This project consists of an ETL (Extract, Transform, Load) job with two phases, data analysis and storage, and a data access component. The overall goal is to process and analyze football data from FBRef.com for Premier League teams. The project utilizes various technologies, including PostgreSQL, Go web server, Airflow, and Kubernetes with Minikube and VirtualBox driver.

## Phase 1: Data Extraction, Transformation, and Loading

In the first phase of the ETL job, data is extracted from FBRef.com, followed by data wrangling and cleaning processes. The cleaned data is then loaded into a PostgreSQL database. This phase involves scraping relevant football data, applying data transformations and data cleaning techniques to ensure data quality, and storing the processed data into the PostgreSQL database.

## Phase 2: Relative Rankings Calculation

The second phase focuses on retrieving the data from the PostgreSQL database and performing calculations to determine relative rankings for midfield, attack, and defense for Premier League football teams. These rankings provide insights into the performance and effectiveness of different aspects of the teams' gameplay. The calculated rankings can be used for further analysis and decision-making.

## Go Rest API

For data access, a Rest API is created using Go and the Gin web framework. The API connects to the PostgreSQL database and provides an interface to retrieve the stored data. This allows users to access the processed data and perform queries or retrieve specific information for further analysis or presentation.

## Orchestration and Infrastructure

The entire project is orchestrated using Apache Airflow, which manages the ETL workflow and schedule. Airflow enables automated execution and monitoring of the ETL tasks, ensuring timely and efficient data processing. The infrastructure is hosted on Kubernetes, specifically utilizing Minikube and the VirtualBox driver for local development. The Airflow instance is created using Helm charts, and a persistent volume and persistent volume claim are provisioned for Airflow DAG storage. The DAG is pre-populated in the persistent volume to ensure availability upon Airflow deployment.

## Architecture Diagram

![alt text](.\Docs\imgs\PremierLeagueStats.jpg)

## Usage

Building the images for the ETL job and RestAPI. We will be pushing it directly to the in-cluster docker daemon on minikube, this helps us build docker images inside the same docker daemon that the minikube uses which means you don’t have to build on your host machine and push the image into a docker registry thus speeds up local experiments.

```bash

    #To point your terminal to use the docker daemon inside minikube run this for powershell
    & minikube -p minikube docker-env --shell powershell | Invoke-Expression

    cd ETL

    #Building ETL Docker image
    docker build -t football_viz .

    cd ../RestApi

    #Building Go RestAPI Docker image
    docker build -t football_viz_api .

```

Create all kubernetes resources.

```bash

    cd kubernetes
    kubectl apply --recursive -f .

```

Below are the commands to populate the persistent volume with the DAG.

```bash

    minikube cp ./ETL/premier_league_dag.py  /home/k8/airflow-data/dags/premier_league_dag.py     

```

Below are the commands to deploy airflow using helm charts.

```bash

    helm upgrade --install airflow apache-airflow/airflow \
    --set executor=KubernetesExecutor \
    --set dags.persistence.enabled=true \
    --set dags.persistence.existingClaim=airflow-dags-persisent-volume-claim \
    --set dags.persistence.storageClassName=local-storage

```

## Conclusion

By leveraging these technologies and infrastructure, the project enables efficient ETL processing, relative rankings calculation, and data access for premier league data analysis. The combination of PostgreSQL, Go web server, Airflow, and Kubernetes provides a scalable, flexible, and robust framework for managing the entire data pipeline and analysis workflow.
