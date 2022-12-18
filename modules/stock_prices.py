import asyncio
from asyncio import sleep
from typing import List
import streamlit as st
from pykafka import KafkaClient
from infrastructure.kafka import KafkaConsumer
from settings import KAFKA_TOPIC_PREFIX


async def stock_price_loader(client: KafkaClient, selected_ticker: str, selected_timeframe: str):
    """
    Функция - загрузчик данных.
    После начальной загрузки данных из Kafka, поддерживает loop до момента отключения клиента или получения команды.
    Завершает работу после выполнения таски consumer.consume
    :param client:
    :param selected_ticker:
    :param selected_timeframe:
    :return: None
    """
    consumer = KafkaConsumer(client, selected_ticker, selected_timeframe)

    async def load():
        st.header(f'Graph for ticker {selected_ticker}')
        await consumer.preload()
        data = consumer.get_data()
        return st.line_chart(data=data)

    def done_callback():
        return

    chart = await load()

    live_data_consumer = asyncio.create_task(consumer.consume(chart))
    live_data_consumer.add_done_callback(done_callback)

    while not (live_data_consumer.cancelled() or live_data_consumer.done() or live_data_consumer.exception()):
        await sleep(1)

    return


def get_available_topics(kafka_topics: list) -> List[str]:
    topics_available = [str(topic, 'UTF-8') for topic in kafka_topics if
                        str(topic, 'UTF-8').startswith(KAFKA_TOPIC_PREFIX)]
    topics_available.sort()
    return topics_available
