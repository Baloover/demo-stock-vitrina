import math
from itertools import islice
from typing import List
import orjson
import pandas as pd
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
from asyncio import sleep
from streamlit.delta_generator import DeltaGenerator

from settings import TIMEFRAME_MAP


class KafkaConsumer:
    def __init__(self, kafka_client: KafkaClient, topic_prefix: str, ticker: str, timeframe: str = 60):
        self._kafka_client: KafkaClient = kafka_client
        self._timeframe: int = TIMEFRAME_MAP.get(timeframe, 60)
        self._topic = self._kafka_client.topics[ticker]
        self._last_topic_offset = self._topic.latest_available_offsets()[0].offset[0]
        self._consumer = self._topic.get_simple_consumer(consumer_timeout_ms=10000,
                                                         auto_offset_reset=OffsetType.LATEST,
                                                         reset_offset_on_start=True,
                                                         )
        self._data: List[List[pd.Timestamp, int]] = []

    async def preload(self) -> None:
        try:
            print("max_points", self._timeframe)
            max_partition_rewind = int(math.ceil(self._timeframe / len(self._consumer._partitions)))
            offsets = []
            for p, op in self._consumer._partitions.items():
                if op.last_offset_consumed - max_partition_rewind > -1:
                    offsets.append((p, op.last_offset_consumed - max_partition_rewind))
                else:
                    offsets.append((p, 0))
            self._consumer.reset_offsets(offsets)
            print("offsets", offsets)
            for message in islice(self._consumer, min(self._last_topic_offset, self._timeframe)):
                message = orjson.loads(message.value)
                self._data.append([pd.to_datetime(message[0]), message[1]])
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self._consumer.stop()
            self._consumer.start()

    async def consume(self, chart: DeltaGenerator) -> None:
        try:
            while True:
                message = self._consumer.consume()
                if message:
                    message = orjson.loads(message.value)
                    try:
                        df = pd.DataFrame([[pd.to_datetime(message[0]), message[1]]], columns=["datetime", "value"])
                        df = df.set_index("datetime")
                        chart.add_rows(df)
                    except:
                        return
                await sleep(0.5)
        except:
            await sleep(1)

    def get_data(self):
        df = pd.DataFrame(self._data, columns=["datetime", "value"])
        df = df.set_index("datetime")
        return df
