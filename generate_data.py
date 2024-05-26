import psycopg2 
import requests
import random
import json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=US'
PARTIES = ["Citizens First Party", "Liberty and Justice Party", "Hope and Change Party"]
STATE = ["California", "Texas", "New York", "Florida"]

random.seed(21)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivered unsuccessfully : {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party VARCHAR(255),
            bio TEXT,
            photo_url TEXT,
            state VARCHAR(255)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )            
    """)

    conn.commit()

def generate_candidate(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f'{user_data['name']['first']} {user_data['name']['last']}',
            'party': PARTIES[candidate_number % total_parties],
            'bio': f'A prestige member of {PARTIES[candidate_number % total_parties]}',
            'photo_url': user_data['picture']['large'],
            'state': STATE[candidate_number % 4],
        }
    else:
        return "Error fetching data from API"
    
def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name":f'{user_data['name']['first']} {user_data['name']['last']}',
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": STATE[random.randint(0, 3)],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }

if __name__ == "__main__":

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

    try:
        conn = psycopg2.connect(f"host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()
        create_tables(conn, cur)

        cur.execute(""" SELECT * FROM candidates""")
        candidates = cur.fetchall()
        if len(candidates) == 0:
            for i in range(12):
                candidate = generate_candidate(i, 3)
                print(candidate)

                cur.execute(
                    """
                    INSERT INTO candidates(candidate_id, candidate_name, party, bio, photo_url, state)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        candidate['candidate_id'], candidate['candidate_name'], candidate['party'], candidate['bio'], candidate['photo_url'], candidate['state']
                    )
                )

                conn.commit()

        for i in range(1000):
            voter_data = generate_voter_data()
            print(voter_data)
            
            cur.execute(
                    """
                    INSERT INTO voters(voter_id, voter_name, date_of_birth, gender, nationality, registration_number, 
                    address_street, address_city, address_state, address_country, address_postcode,
                    email, phone_number, picture, registered_age)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        voter_data['voter_id'], voter_data['voter_name'], voter_data['date_of_birth'], voter_data['gender'], voter_data['nationality'], voter_data['registration_number'],
                        voter_data['address']['street'], voter_data['address']['city'], voter_data['address']['state'], voter_data['address']['country'], voter_data['address']['postcode'],
                        voter_data['email'], voter_data['phone_number'], voter_data['picture'], voter_data['registered_age']
                    )
            )

            conn.commit()

            producer.produce(
                'voters_topic',
                key=voter_data['voter_id'],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )
            print("Produced voter {}, data: {}".format(i, voter_data))
            producer.flush()




    except Exception as e:
        print("Error : ", e)
