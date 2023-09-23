import argparse
import os
import uuid

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class User(object):
    def __init__(self, name=None, favorite_number=None, favorite_color=None):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color


def dict_to_user(obj,ctx):
    if obj is None:
        return None
    return User(name=obj['name'],
                favorite_number=obj['favorite_number'],
                favorite_color=obj['favorite_color'])

def main():
    topic = "t_raw"
    is_specific = "true"

    if is_specific:
        schema = "user_specific.avsc"
    else:
        schema = "user_generic.avsc"

    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema}") as f:
        schema_str = f.read()

    sr_conf = {'url': 'http://docker_kafka-schema-registry-1:8081'}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_user)
                                         

    consumer = Consumer({
        'bootstrap.servers': 'b-2.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096,b-3.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdev.bzswyw.c4.kafka.eu-west-3.amazonaws.com:9096',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'coinscrap',
        'sasl.password': 'iAxAMDTL8atzac',
        'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
        'auto.offset.reset': 'earliest',
    })
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if user is not None:
                print("User record {}: name: {}\n"
                      "\tfavorite_number: {}\n"
                      "\tfavorite_color: {}\n"
                      .format(msg.key(), user.name,
                              user.favorite_number,
                              user.favorite_color))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    parser.add_argument('-p', dest="specific", default="true",
                        help="Avro specific record")

    main(parser.parse_args())
