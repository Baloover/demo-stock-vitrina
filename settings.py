from envparse import Env

env = Env()
# env.read_envfile()

TIMEFRAME_MAP = {
    "1m": 60,
    "1h": 3600,
    "4h": 14400
                 }
####################################
# Kafka
####################################
KAFKA_URL = env('KAFKA_URL', default='localhost:9092')
KAFKA_TOPIC_PREFIX = env('KAFKA_TOPIC_PREFIX', default='dev_')

