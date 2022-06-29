FROM apache/airflow:2.3.2

# Install various linux packages
USER root
RUN apt-get update \
 && apt-get install -y \
    # To run `ping <servername/ip>` from container
    iputils-ping \
    # To run postgres mainetnance on dkraapp04, e.g. pg_dump for backups 
    postgresql-client \
    vim \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# Store aws credentials in containers
RUN mkdir ~/.aws
COPY aws_credentials /home/airflow/.aws/credentials
ENV AWS_DEFAULT_REGION=eu-central-1

# Install requirements form dag repo
# IMORTANT: Make sure to clone `git clone https://github.com/BrejnholtIT/dags.git` beforehand
# or for empty installation place requirements.txt in dags folder.
COPY dags/requirements.txt .
RUN pip install -r requirements.txt