# demo-stock-vitrina
====================================================
Витрина для просмотра сгенерированных биржевых данных.
<br>
В качестве источника - Kafka.<br>
Данные можно запрапрашивать за период до 4 часов.

Запуск проекта
-----------------------------
Для запуска необходимо передать две переменные окружения:

    KAFKA_TOPIC_PREFIX, по умолчанию - 'dev_'
    KAFKA_URL по умолчанию - 'localhost:9092'


Команда для локального запуска:

    streamlit run main.py


Docker:

    docker run -e KAFKA_URL=localhost:9092 -e KAFKA_TOPIC_PREFIX=dev_ -p 8501 baloover/stock-front-proxy:0.0.0

Файловая структура
-----------------------------
| File             | Contents                                           |
|------------------|----------------------------------------------------|
| main.py          | Основной файл витрины, используется для её запуска |
| settings.py      | Файл конфигурации и настроек                       |
| moduled/         | Бизнес-функции хранятся тут                        |
| -stock_prices.py | функционал работы с котировками                    |
| infrastructure/  |                                                    |
| -kafka.py        | функционал работы с Kafka                          |
