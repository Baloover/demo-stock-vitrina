import math
from itertools import islice
from typing import List
import orjson
import pandas as pd
from pandas import DataFrame
from pykafka import KafkaClient
from pykafka.common import OffsetType
from pykafka.exceptions import SocketDisconnectedError, LeaderNotAvailable
from settings import TIMEFRAME_MAP


class KafkaConsumer:
    def __init__(self, kafka_client: KafkaClient, ticker: str):
        """
        Класс отвечает за загрузку данных с Kafka.
        preload - Загружает исторические данные в с соответствии с timeframe (если имеются)
        consume_singe_message - загружает одно сообщение (каждый раз следующее)
        :param kafka_client:
        :param ticker:
        :param timeframe:
        """
        self._kafka_client: KafkaClient = kafka_client
        self._ticker = ticker
        topic = self._kafka_client.topics[ticker]
        self._last_topic_offset = topic.latest_available_offsets()[0].offset[0]
        self._consumer = topic.get_simple_consumer(consumer_timeout_ms=10000,
                                                         auto_offset_reset=OffsetType.LATEST,
                                                         reset_offset_on_start=True,
                                                         )

    async def preload(self, timeframe: str = 60) -> DataFrame:
        timeframe: int = TIMEFRAME_MAP.get(timeframe, 60)
        data: List[List[pd.Timestamp, int]] = []
        try:
            max_partition_rewind = int(math.ceil(timeframe / len(self._consumer._partitions)))
            offsets = []
            for p, op in self._consumer._partitions.items():
                if op.last_offset_consumed - max_partition_rewind > -1:
                    offsets.append((p, op.last_offset_consumed - max_partition_rewind))
                else:
                    offsets.append((p, 0))
            self._consumer.reset_offsets(offsets)
            for message in islice(self._consumer, min(self._last_topic_offset, timeframe)):
                message = orjson.loads(message.value)
                data.append([pd.to_datetime(message[0]), message[1]])
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self._consumer.stop()
            self._consumer.start()
        df = pd.DataFrame(data, columns=["datetime", self._ticker])
        df = df.set_index("datetime")
        return df

    async def consume_singe_message(self):
        try:
            message = self._consumer.consume()
        except (SocketDisconnectedError, LeaderNotAvailable) as e:
            self._consumer.stop()
            self._consumer.start()
            return e
        return message
