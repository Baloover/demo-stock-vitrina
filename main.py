import streamlit as st
import asyncio
from pykafka import KafkaClient
from modules.stock_prices import stock_price_loader, get_available_topics
from settings import KAFKA_URL


st.set_page_config(page_title='Faked stock data demo',
                   layout="centered",
                   initial_sidebar_state="auto"
                   )
st.title('Faked stock data demo')
col1, col2 = st.columns(2)

loop = asyncio.new_event_loop()
client = KafkaClient(KAFKA_URL)

topics_available = get_available_topics(client.topics)

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

loop.run_until_complete(stock_price_loader(client, selected_ticker, selected_timeframe))

