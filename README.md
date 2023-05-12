# riskthinking_assesment

## Deployment Instructions

### Airflow deployment

Once the repository has been cloned to a host machine. From the root directory run the following commands from a terminal.

`cd riskthinking_assesment/airflow_docker/`

`docker build . --tag airflow_ext:latest`

`docker compose up airflow-init`

`docker compose up`

This will initiate the airflow deployment. 

### Flask Server deployment

The flask server is deployed on Heroku and the predict endpoint can be accessed at :
To build and run the flask deployment on a local envionment run the following commands from a terminal, starting at the root directory of this repository.

`cd riskthinking_assesment/flask-server/`

`docker compose up`
