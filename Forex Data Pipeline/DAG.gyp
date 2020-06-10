from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor # TASK 1
from airflow.contrib.sensors.file_sensor import FileSensor # TASK 2
from airflow.operators.python_operator import PythonOperator # TASK 3
from airflow.operators.bash_operator import BashOperator # TASK 4
from airflow.operators.hive_operator import HiveOperator # TASK 5
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator # TASK 6
from airflow.operators.email_operator import EmailOperator # TASK 7
from airflow.operators.slack_operator import SlackAPIPostOperator #TASK 8


import csv
import requests
import json

default_args = {                            # We send a dictionary with default arguments as a parameter
    "owner" :  "airflow",
    "start_date" : datetime(2020, 6, 8),    #If we specify a past day, the DAG will be triggered inmediately
    "depends_on_past" : False,              #Indicates that even if the past task failed, this one will run anyway 
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email" : "youremail@host.com",         # Since on this lab we are not using email notification, this value does not matter
    "retries" : 1,                          # Task should be restarted at most one time after a failure
    "retry_delay" : timedelta(minutes=5)    # Scheduler should wait 5 minutes before restarting that task
}

# Instatiation of the object using default_args
# These are the minimum parameter that should be used
# Using "with" function in python allow us to well close the DAG when is not used anymore in the code
with DAG(dag_id="forex_data_pipeline", 
         schedule_interval="@daily", 
         default_args=default_args, 
         catchup=False) as dag:

##################################### TASK 3 - API DATA DOWNLOAD #####################################
    
    # Use of this Function is below TASK 2

    # Function created to be used in the PythonOperator:
        # Download forex rates according to the currencies we want to watch
        # described in the file forex_currencies.csv
    def download_rates():
        with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
            reader = csv.DictReader(forex_currencies, delimiter=';')
            for row in reader:
                base = row['base']
                with_pairs = row['with_pairs'].split(' ')
                indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
                outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
                for pair in with_pairs:
                    outdata['rates'][pair] = indata['rates'][pair]
                with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                    json.dump(outdata, outfile)
                    outfile.write('\n')
    
##################################### TASK 1 - HTTP SENSOR OPERATOR (FOREX DATA PIPELINE) #################################

    # A SensorOperator is used to test the API and see if it is working as expected
    # Operator used -> https://airflow.apache.org/docs/stable/_modules/airflow/sensors/http_sensor.html
    # Connection created in the Airflow UI -> "forex_api"
    # Link for API -> https://api.exchangeratesapi.io/latest?base=USD
    # Which is extracted from https://exchangeratesapi.io/
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        method='GET',
        http_conn_id='forex_api', # This is the name of the connection to be create in airfolow (instead of adding the API link)
        endpoint='latest',
        response_check=lambda response: "rates" in response.text, # If "rates" exist on response.text it will return True
        poke_interval=5, # The http sensor will send an http request every 5 seconds
        timeout=20 # During at most 20 seconds before timing out
    )
    # For testing this, it was necesarry to run the ./start.sh in the section3 folder
    # Once it finishes (took a while) we check al the docker containers created with -> docker ps
    # We log in into the one that was releated to airflow -> "airflow-section-3_airflow" using the CONTAINER_ID
    # after that we ran -> airflow test forex_data_pipeline is_forex_rates_available 2020-06-08
        # The firs parameter if the python script
        # The second one was the instance call created for test the API
        # The last one was a date in the past so the task got executed
        # RESULT -> [2020-06-09 18:14:28,367] {base_sensor_operator.py:123} INFO - Success criteria met. Exiting.


##################################### TASK 2 - FILE SENSOR OPERATOR #####################################

    # A file sensor will look for a file stored (in this case in the docker container) and see if it exists
    # Operator used -> https://airflow.apache.org/docs/stable/_modules/airflow/contrib/sensors/file_sensor.html
    # Connection created in the Airflow UI -> "forex_path"

    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20 
    )
    # After this, if running "docker ps" do not appear the same docker container as before (airflow-section-3_airflow)
        # The shell script ./restart.sh should be run 
    # If we run "docker logs <CONTAINER_ID>" we can get the files analyzed and see if there is errors in the .py scripts
    # docker exec -it <CONTAINER_ID> /bin/bash
    # airflow test forex_data_pipeline is_forex_currencies_file_available 2020-06-08


##################################### TASK 3 - API DATA DOWNLOAD #####################################

    # Download Forex data (forex rates) from the API
    # Operator used -> https://airflow.apache.org/docs/stable/howto/operator/python.html

    downloading_rates = PythonOperator(
        task_id='downloading_rates',
        python_callable=download_rates      # Function created adove
    ) 

    # airflow test forex_data_pipeline downloading_rates 2020-06-08
    # New file was created: /usr/local/airflow/dags/files/forex_rates.json


##################################### TASK 4 - SAVING DATA IN HDFS #####################################

    # Saving the forex rates in HDFS

    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

    # airflow test forex_data_pipeline saving_rates 2020-06-08 
        # Marking task as SUCCESS.dag_id=forex_data_pipeline
    # HUE is a tool for seeing HDFS files :O
    # http://localhost:32762/  
        # Credentials: 
            # username = root
            # password = root


##################################### TASK 5 - REQUESTING DATA WITH HIVE #####################################

    # Operator used -> https://airflow.apache.org/docs/stable/_modules/airflow/operators/hive_operator.html
    # "hive_conn" connection was created in Airflow UI
    # There is a container for HIVE, by clicking "docker ps" we are able to see the port assigned to each container
        # That port was used in the connection creation in airflow

    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # airflow test forex_data_pipeline creating_forex_rates_table 2020-06-08
    # http://localhost:32762/hue/ -> In HIVE database the table (withou data) will be locate


##################################### TASK 6 - PROCESSING WITH SPARK #####################################

    # Operator used -> https://airflow.apache.org/docs/stable/_api/airflow/contrib/operators/spark_submit_operator/index.html
    # File (Spark application script) to be used is located in 
    # Spark spark_conn was created in Airflow

    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/forex_processing.py",
        verbose=False
    )

    # airflow test forex_data_pipeline forex_processing 2020-06-08

##################################### TASK 7 - SENDING EMAIL NOTIFICATION #####################################

    # Operator used -> 
    # gmail password dkqoxscizxvtamig obtained from https://myaccount.google.com/apppasswords
        # Just needed to login with personal accoount, and the select the device (which was mac for my case)
    # Is is needed to configure the host in the /usr/local/airflow/airflow.cfg
        # Look for [smtp] and change the configuration

        # After this was done:
            # smtp_host = smtp.gmail.com
            # smtp_starttls = True
            # smtp_ssl = False
            # # Uncomment and set the user/pass settings if you want to use SMTP AUTH
            # smtp_user = miguel.biomedic@gmail.com
            # smtp_password = dkqoxscizxvtamig # Password generated in https://myaccount.google.com/apppasswords
            # smtp_port = 587
            # smtp_mail_from = miguel.biomedic@gmail.com
        
        # It is necessary to restart the container 
            # docker-compose restart airflow
            # docker ps (Wait until the docker is healthy again)

    # Operator implementation
    sending_email_notification = EmailOperator(
        task_id="sending_email_notification",
        to="airflow_course@yopmail.com",
        subject="forex_data_pipeline",
        html_content="<h3>forex_data_pipeline succeeded</h3>"
    )

    # airflow test forex_data_pipeline sending_email_notification 2020-06-08

##################################### TASK 8 - SENDING SLACK NOTIFICATION #####################################

    # It is necessary to create a slack account
    # Slack channels details:
        # USERNAME -> miguel.biomedic@gmail.com
        # EQUIPO -> OJEDA_IT
        # PROYECTO -> learning
    # API Used -> https://api.slack.com/apps
        # App "airflow" was created on this website

    # Operator used -> https://airflow.apache.org/docs/stable/_modules/airflow/operators/slack_operator.html

    sending_slack_notification = SlackAPIPostOperator(
        task_id="sending_slack_notification",
        # The token was generated in the app page -> https://api.slack.com/apps/A0154EN6ZUJ/oauth?
        # After adding a new token scope, the value was generated
        token="xoxp-1198326019776-1174489516066-1159775872839-f00d793fc12a6c167ba89856e9bd1281",
        username="airflow",
        text="DAG forex_data_pipeline: DONE",
        channel="#airflow-exploit"
    )

    # airflow test forex_data_pipeline sending_slack_notification 2020-06-08


##################################### DEPENDENCIES #####################################

is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates >> saving_rates >> creating_forex_rates_table >> forex_processing >> sending_email_notification >> sending_slack_notification