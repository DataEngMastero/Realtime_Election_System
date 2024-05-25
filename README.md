# Realtime_Election_System


Setup venv for the project: python -m venv venv <br>
Downloading Required packages: pip install <br>
psycopg2-binary confluent_kafka simplejson <br>

Setting up Infrastructure : docker-compose.yml <br>
Running docker: docker-compose up -d <br>

Running the following command to check if broker is  healthy and accepting requests <br>
kafka-topics --list --bootstrap-server broker:29092 <br>

generate_data.py <br>
1. Connect with Postgres DB 
2. Creating Tables: voter, votes, candidate
3. Generating the data into our system using Ramdom API for all three tables.

Resources: <br>
1. https://www.youtube.com/watch?v=X-JnC9daQxE
