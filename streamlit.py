import streamlit as st
import time 
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import matplotlib.pyplot as plt
# from streamlit_autorefresh import st_autorefresh

# st_autorefresh(interval=2000, limit=100, key="fizzbuzzcounter")
st.title("Realtime Election Dashboard")

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(f"host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # fetch total numbers of voters
    cur.execute(""" SELECT COUNT(*) FROM voters;""")
    voters_cnt = cur.fetchone()[0]

    # fetch total numbers of candidates
    cur.execute(""" SELECT COUNT(*) FROM candidates;""")
    candidates_cnt = cur.fetchone()[0]

    return voters_cnt, candidates_cnt

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)

    return data

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch Voting Statistics
    voters_cnt, candidate_cnt = fetch_voting_stats()

    # Display the Statistics
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_cnt)
    col2.metric("Total Candidates", candidate_cnt)

    topic_name = 'aggregated_votes_per_candidate'
    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)
    
    results = pd.DataFrame(data)
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party'])
        st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

    # Display statistics and visualizations
    state_results = pd.DataFrame(data)
    state_results = state_results.loc[state_results.groupby('state')['total_votes'].idxmax()]
    first_state = state_results.iloc[0]
    sec_state = state_results.iloc[1]
    third_state = state_results.iloc[2]
    fourth_state = state_results.iloc[3]
    

    st.markdown("""---""")
    st.header('State Leading Candidate')
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.subheader(first_state['state'])
        st.image(first_state['photo_url'], width=150)
        st.text(first_state['candidate_name'])
        st.text(first_state['party'])
        st.text("Total Vote: {}".format(first_state['total_votes']))

    with col2:
        st.subheader(sec_state['state'])
        st.image(sec_state['photo_url'], width=150)
        st.text(sec_state['candidate_name'])
        st.text(sec_state['party'])
        st.text("Total Vote: {}".format(sec_state['total_votes']))

    with col3:
        st.subheader(third_state['state'])
        st.image(third_state['photo_url'], width=150)
        st.text(third_state['candidate_name'])
        st.text(third_state['party'])
        st.text("Total Vote: {}".format(third_state['total_votes']))

    with col4:
        st.subheader(fourth_state['state'])
        st.image(fourth_state['photo_url'], width=150)
        st.text(fourth_state['candidate_name'])
        st.text(fourth_state['party'])
        st.text("Total Vote: {}".format(fourth_state['total_votes']))


    # Fetch data from Kafka on aggregated turnout by location
    st.markdown("""---""")
    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)

    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)
    location_result_sorted = location_result.sort_values(by='count', ascending=False)
    print(location_result_sorted)

    # Display location-based voter information with pagination
    st.header("Voters by Location")
    st.bar_chart(location_result_sorted, x="state", y="count", color="state")

    st.markdown("""---""")
    age_consumer = create_kafka_consumer("aggregated_turnout_by_age")
    age_data = fetch_data_from_kafka(age_consumer)
    age_result = pd.DataFrame(age_data)

    age_result = age_result.loc[age_result.groupby('registered_age')['count'].idxmax()]
    age_result = age_result.reset_index(drop=True)
    print(age_result)

    bins = [10, 20, 30, 40, 50, 60, 70]
    labels = ['0-19','20-29', '30-39', '40-49', '50-59', '60-69']

    age_result['age_bucket'] = pd.cut(age_result['registered_age'], bins=bins, labels=labels)
    age_bucket_counts = age_result.groupby('age_bucket')['count'].sum()
    age_bucket_counts = age_bucket_counts.reset_index(drop=True)
    print(age_bucket_counts)
    print(age_bucket_counts.index)

    st.header("Voters by Age")
    fig1, ax1 = plt.subplots()
    ax1.pie(age_bucket_counts, labels=labels, autopct='%1.1f%%', startangle=140)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    st.pyplot(fig1)

    # Update the last refresh time
    st.session_state['last_update'] = time.time()

update_data()