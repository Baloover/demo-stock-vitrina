from asyncio import sleep
import streamlit as st
import asyncio
from infrastructure.kafka import KafkaConsumer
from pykafka import KafkaClient
from settings import KAFKA_URL, KAFKA_TOPIC_PREFIX


st.set_page_config(page_title='Faked stock data demo', page_icon=None, layout="centered", initial_sidebar_state="auto", menu_items=None)
st.title('Faked stock data demo')
col1, col2 = st.columns(2)

loop = asyncio.new_event_loop()
client = KafkaClient(KAFKA_URL)
topics_available = [str(topic, 'UTF-8') for topic in client.topics if str(topic, 'UTF-8').startswith(KAFKA_TOPIC_PREFIX)]
topics_available.sort()

with col1:
    selected_ticker = st.selectbox(
        'Ticker selector',
        topics_available
    )

with col2:
    selected_timeframe = st.selectbox(
        'Preload interval selector',
        ('1m', '1h', '4h'),
    )

button = st.button("Load Data")


async def periodic():
    consumer = KafkaConsumer(client, KAFKA_TOPIC_PREFIX, selected_ticker, selected_timeframe)

    async def load():
        st.header(f'Graph for ticker {selected_ticker}')
        await consumer.preload()
        data = consumer.get_data()
        return st.line_chart(data=data)

    def done_callback():
        return None

    chart = await load()

    task = asyncio.create_task(consumer.consume(chart))
    task.add_done_callback(done_callback)

    while not (task.cancelled() or task.done()):
        await sleep(1)

    return

loop.run_until_complete(periodic())
