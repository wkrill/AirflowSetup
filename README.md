# Airflow Setup
This repository contains a full configuration of Airflow using docker-compose for installation on a new machine.

## Features
- Airflow 2.3
- LocalExecutor for simple parallel task execution
- SMTP setup for email notifications
- Custom XCOM Backend for passing dataframes between tasks
- Secrets Backend for safe and easy maintenance of connections (using AWS Secrets Management)
- Utility library with custom operators

## Before you begin
- Install Docker and Docker-Compose (see [this guide](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#before-you-begin))
- On Windows you will want to use [Docker Desktop](https://docs.docker.com/desktop/windows/install/) with the [WSL 2 based engine](https://docs.docker.com/desktop/windows/wsl/). When installed and enabled copy `.wslconfig` to your home folder.
- Get access an AWS Account with permissions to S3 and AWS Secrets Manager

## Installation
Put your AWS credentials in `airflow/aws_credentials` (see the `aws_credentials.example` file for an example).
Open a command prompt in this directory and run `docker build .` to build the custom docker image from the `Dockerfile` file.
Then run `docker-compose build` to build the compose app based off of the `docker-compose.yml` file.

To start Airflow run `docker-compose up -d`.
You should see the following components start:
- a postgres database: the meta database for Airflow
- a scheduler: the machinary that executes tasks
- a webserver: a Web UI to interact with Airflow and monitor DAG executions (by default running on localhost port 8080)
- a CLI: a command line interface to interact with Airflow.
- a triggerer: mechanism for asyncroneous task execution (advanced feature)

When Airflow is running you can access the Web UI from a browser: [http://localhost:8080](http://localhost:8080).
The default credentials for the UI are simply: userame = airflow, password = airflow.

To check the health of each component run `docker-compose ps` or open Docker Desktop.
To stop Airflow again run `docker-compose down`.

## Development and Deployment
To develop dags folow these guidelines:
- Create and checkout to a new branch e.g. named `my_dag_branch`: `git checkout -b my_dag_branch`.
- Create the dag code file `airflow_setup/dags/my_dag.py`
- When you have defined a valid DAG, the scheduler will register it and it will be listed in the UI main page (this can take up to 30 sec). 
- You can then execute it and look at the logs from the UI. 
- Commit your changes on the `my_dag_branch` branch.
- When you are ready to put your dag in production, make sure your branch is up to date with `main` branch: `git merge main`.
- Merge the changes in to main branch: `git check out main` and `git merge my_dag_branch`.
- Deploy: `git push`. The changes will then be pulled to the server. 
- When you branch is merged into main you can delete it (`git branch --delete my_dag_branch`)
- If you have defined a schedule, make sure to turn it off in the UI of your local Airflow instance - otherwise it might be run accidentially from your PC when you start Airflow up next time. 

The dag should appear in the Web UI for the production environment on http://dkraapp04:8080/ within a few minutes.

> :warning: **Do not push unfinished work to `main` branch**: When work is pushed to the `main` branch, it will automatically be pulled into the production environment! Therefore, make sure to to work on a separate branch while developing and testing. 

> **How are changes available in the containers?** The directories `dags`, `logs`, `plugins` and `include` are so called *volumes*, i.e. they are accessible from both inside and outside the containers. This makes it possible to develop outside the containers in any IDE you like.

## Connecting to external sources
Airflow is build to connect to external systems, and for this it uses *hooks*. These makes it very easy and secure to connect from a DAG script. 
E.g. the following lines fetches some data in as4data and stores it in a pandas dataframe:
```
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="as4data")
df = hook.get_pandas_df('select * from reporting.dim_varstam limit 10')
```
This assumes a connection named `as4data` with credentials to the database is configured.

## Configuring connections
Connections can be created and configured in the following ways
- In the Web UI under *Admin > Connections*.
- In [AWS Secret Manager](https://eu-central-1.console.aws.amazon.com/secretsmanager/home?region=eu-central-1#!/listSecrets/)
- With environment variables defined in Dockerfile of dockercompose (not recomended).

The Web UI is easiest and preffered for development. However, to make a connection available in all Airflow environments (production and other development envs) the connection can be stored in AWS Secrets Manager.
The connection is then stored as a secret named `airflow/connections/<my_connection_name>`. The prefix `airflow/connections/` is needed for Airflow to register it as a connection.

For more information see [Secrets Backend Documentation](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/secrets-backends/aws-secrets-manager.html).



## Changing configurations
If you change configurtations in Dockerfile or docker-compose.yml (e.g. upgrading to a newer version of Airflow), you need to restart the app with the commands:
```
docker-compose down
docker-compose build .
docker-compose up -d
```

## Entering a container
Airflow runs in *containers* which isolate the environment from the host machine.
However sometimes you need access to the environment, e.g. to access connections when testing out a hook. 
Here you need to enter a container enter a container by running the command `sudo docker exec -it <container_name> bash`. This can be achieved in VS Code using the *Remote Explorer* extension.

From inside an airflow container, you can enter a python shell by typing `python`, and import and execute hooks and anythin available in DAG scripts. 

You can also access the Airflow CLI (an alternative to the Web UI). Type `airflow -h` for info. To exit the container again press Ctrl+D.

## Production Environment
Airflow is running on the linux server `dkraapp04` and located in `/home/datauser/airflow-docker`.
You will want to log in to troubleshoot the app (e.g. for health checks `docker-compose ps`) or when you want to change configurations or upgrade to a newer version of Airflow.

> **Note**: On the server you might need root permissions for some commands (here you need to type `sudo` before the command).

### Automatic pulls
Changes that are pushed to the `main` branch will automatically be pulled to `/home/datauser/airflow-docker` at the linux server. This is currently set up with a cron job. You can verify that changes are pulled by reading `airflow_docker/pull-log.txt`. This is how dags are deployed. 

> **Note**: Updates to Dockerfile or docker-compose.yml that are pushed to `main` branch do not take effect in production, before you restart the app on the server as explained in the *Changing configurations* section.


## Resources
- [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Guides](https://www.astronomer.io/guides/)
- [Webinars](https://www.astronomer.io/events/webinars/)
- [Operator Registry - browse custom operators](https://registry.astronomer.io/)


## TODO
- Makefile/Invoke.py to simplify docker commands
- Script to upload connections stored in metadatabase to AWS Secrets Manager (so you can create you connections in the UI and afterwards share them in AWS).
