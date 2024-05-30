# Realtime_Election_System

### Overview
This project is a realtime streaming pipeline that powers streamlit dashboard for getting fetching realtime voting statistics.
There are three party - ["Citizens First Party", "Liberty and Justice Party", "Hope and Change Party"]
Each party with one candidate in four states: ["California", "Texas", "New York", "Florida"]

### Dashboard Image: 



Entities - Voters, Candidates and Votes

### Steps taken for the Project
1. Setup venv for the project: <b> python -m venv venv </b>
2. Downloading Required packages: <b>pip install psycopg2-binary confluent_kafka simplejson </b>
3. Setting up Infrastructure : docker-compose.yml <br>
   Running docker: <b> docker-compose up -d </b>
4. Python scripts - <br>

generate.py - <br>
    Generates Candidate & Voters Information using Random User API for US nationality.  <br>
    Simultaneously inserting into Postgres DB and publishing voters info into Kafka topic (voters_topic). <br>

voting.py -  <br>
    Voters information are consumed from Kafa topic (voters_topic).  <br>
    Randomly voters select candidate as per their state and pushed into another Kafka topic (votes_topic) <br>

spark-streaming.py -  <br>
    Creats spark session that reads stream from votes_topic and do pre-processing and aggregations.  <br> These aggregations are written back to respective Kafka topics. <br>

streamlit.py -  <br> 
    Creates streamlit application and refreshes to fetch the most recent data.  <br>



### Tech Stack
1. Python
2. Postgres
3. Kafka 
4. Spark
5. Streamlit


### Resources: <br>
https://www.youtube.com/watch?v=X-JnC9daQxE
 
