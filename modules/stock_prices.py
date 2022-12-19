import asyncio
from asyncio import sleep
from typing import List
import orjson
import pandas as pd
import streamlit as st
from pykafka import KafkaClient
from infrastructure.kafka import KafkaConsumer
from settings import KAFKA_TOPIC_PREFIX


async def stock_price_loader(client: KafkaClient, selected_ticker: str, selected_timeframe: str):
    """
    Функция - загрузчик данных.
    После начальной загрузки данных из Kafka, поддерживает loop до момента отключения клиента или получения команды.
    Завершает работу после выполнения таски consumer.consume
    Если consumer.consume был завершен с ошибкой скрипт будет перевыполнен полностью, включая запрос исторических данных
    :param client:
    :param selected_ticker:
    :param selected_timeframe:
    :return: None
    """
    consumer = KafkaConsumer(client, selected_ticker)

    async def stock_price_preload():
        st.header(f'Graph for ticker {selected_ticker}')
        data = await consumer.preload(selected_timeframe)
        return st.line_chart(data=data)

    chart = await stock_price_preload()

    async def stock_price_live_update() -> None:
        try:
            while True:
                message = await consumer.consume_singe_message()
                if message:
                    message = orjson.loads(message.value)
                    try:
                        df = pd.DataFrame([[pd.to_datetime(message[0]), message[1]]],
                                          columns=["datetime", selected_ticker])
                        df = df.set_index("datetime")
                        chart.add_rows(df)
                    except:
                        return
                await sleep(0.5)
        except:
            await sleep(1)

    def done_callback(func):
        print(func)
        return

    live_data_consumer = asyncio.create_task(stock_price_live_update())
    live_data_consumer.add_done_callback(done_callback)

    while not (live_data_consumer.cancelled() or live_data_consumer.done()):
        await sleep(1)

    return


def get_available_topics(kafka_topics: list) -> List[str]:
    topics_available = [str(topic, 'UTF-8') for topic in kafka_topics if
                        str(topic, 'UTF-8').startswith(KAFKA_TOPIC_PREFIX)]
    topics_available.sort()
    return topics_available
