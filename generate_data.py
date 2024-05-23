import psycopg2 
import requests
import random

BASE_URL = 'https://randomuser.me/api/?nat=US'
PARTIES = ["Citizens First Party", "Liberty and Justice Party", "Hope and Change Party"]
STATE = ["California", "Texas", "New York", "Florida"]

random.seed(21)

class Connection:
    def __init__(self, host, dbname, user, password):
        self.conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
        self.cur = self.conn.cursor()

def create_tables(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party VARCHAR(255),
            bio TEXT,
            photo_url TEXT
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
            'photo_url': user_data['picture']['large']
        }
    else:
        return "Error fetching data from API"
    

if __name__ == "__main__":
    try:
        postgres_conn = Connection("localhost", "voting", "postgres", "postgres")
        conn, cur = postgres_conn.conn, postgres_conn.cur
        create_tables(conn, cur)

        cur.execute(""" SELECT * FROM candidates""")
        candidates = cur.fetchall()
        print(candidates)

        if len(candidates) == 0:
            for i in range(12):
                candidate = generate_candidate(i, 3)
                print(candidate)

                cur.execute(
                    """
                    INSERT INTO candidates(candidate_id, candidate_name, party, bio, photo_url)
                    VALUES(%s, %s, %s, %s, %s)
                    """,
                    (
                        candidate['candidate_id'], candidate['candidate_name'], candidate['party'], candidate['bio'], candidate['photo_url']
                    )
                )

                conn.commit()

    except Exception as e:
        print("Error : ", e)
