import psycopg2
import json
import time
import random 
from datetime import datetime
from confluent_kafka import SerializingProducer, Consumer, KafkaError, KafkaException

kafka_conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(kafka_conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(kafka_conf)


def filter_candidates_by_state(candidates, state):
    for record in candidates:
        if record[0] == state:
            return record[1]
    return None

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivered unsuccessfully : {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    conn = psycopg2.connect(f"host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    cur.execute(""" 
        SELECT 
            state, 
            JSON_AGG(ROW_TO_JSON(col)) AS candidates
        FROM (
            SELECT * FROM candidates
        ) col
        GROUP BY state;
                
    """)
    candidates = cur.fetchall()

    if len(candidates) == 0:
        raise Exception("No candidates found in the database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(filter_candidates_by_state(candidates,voter['address']['state']))
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }

                try:
                    print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")
                    cur.execute("""
                        INSERT INTO votes(voter_id, candidate_id, voting_time)
                        VALUES(%s, %s, %s)
                    """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)

                except Exception as e:
                    print("Error ::: ", e)
            
            time.sleep(0.5)
            
    except Exception as e:
        print("Error >>> ", e)




