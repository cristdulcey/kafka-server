import os

from kafka import KafkaConsumer

list_servers = os.environ.get('BOOTSTRAP_SERVERS', "").split(",")
BOOTSTRAP_SERVERS = list_servers
BOOTSTRAP_TOPIC = os.environ.get('BOOTSTRAP_TOPIC', "")


class MyKafkaConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            *BOOTSTRAP_TOPIC.split("-"),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'{BOOTSTRAP_TOPIC}-group',
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )

    def consume_messages(self):
        try:
            topics = self.consumer.topics()
            print(f"Ready to consume {topics} from {BOOTSTRAP_SERVERS}")
            for m in self.consumer:
                print(f'topic {m.topic}\nkey {m.key}\nvalue {m.value}\ntimestamp {m.timestamp}\n')
        except Exception as error:
            print(f'error consumer: {error}')
            self.consumer.close()


if __name__ == '__main__':
    consummer_obj = MyKafkaConsumer()  # pragma: no cover
    consummer_obj.consume_messages()  # pragma: no cover
