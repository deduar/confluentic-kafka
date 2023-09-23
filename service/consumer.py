import uuid
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

class Consumer:

    def __init__(self) -> None:
        pass

    def error_cb(self, err):
        print("Client error: {}".format(err))
        if err.code() == KafkaError._ALL_BROKERS_DOWN or \
        err.code() == KafkaError._AUTHENTICATION:
            # Any exception raised from this callback will be re-raised from the
            # triggering flush() or poll() call.
            raise KafkaException(err)
        
    def consumer(self):     
        return Consumer({
            'bootstrap.servers': 'b-2.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096,b-3.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'security.protocol': 'SASL_SSL',
            'sasl.username': 'coinscrap',
            'sasl.password': 'iAxAMDTL8atzac',
            'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
            'auto.offset.reset': 'earliest',
            'error_cb': error_cb,
        })
    
    def subscribe(self,c:Consumer,topic:str):
        return c.subscribe([topic])
    
    def cosume_topic(self,c:Consumer):
        try:
            while True:
                msg = c.poll(0.1)  # Wait for message or event/error
                if msg is None:
                    continue
                if msg.error():
                    print('Consumer error: {}'.format(msg.error()))
                    continue
                print('Consumed: {}'.format(msg.value()))
        except KeyboardInterrupt:
            pass