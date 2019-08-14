from time import sleep
from json import dumps
from numpy.random import choice, randint
from kafka import KafkaProducer


def get_random_value():
    """
    Generate dummy data
    :return: dict
    """
    cities_list = ['Lviv', 'Kyiv', 'Odessa', 'Donetsk']
    currency_list = ['HRN', 'USD', 'EUR', 'GBP']

    return {
        'city': choice(cities_list),
        'currency': choice(currency_list),
        'amount': randint(-100, 100)
    }


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             compression_type='gzip')
    topic = 'transaction'

    while True:
        for _ in range(100):
            data = get_random_value()
            try:
                future = producer.send(topic=topic, value=data)
                record_metadata = future.get(timeout=10)

                print('--> The message has been sent to a topic: {}, partition: {}, offset: {}'
                      .format(record_metadata.topic, record_metadata.partition, record_metadata.offset))
            except Exception as e:
                print('--> Error occured: {}'.format(e))
            finally:
                producer.flush()

        sleep(1)
