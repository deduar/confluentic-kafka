from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat
import uuid


def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
 Options:
  -T <intvl>   Enable client statistics at specified interval (ms)
'''
    sys.stderr.write(options)
    sys.exit(1)


if __name__ == '__main__':

    conf = {
        'bootstrap.servers': 'b-3.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-2.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'coinscrap',
        'sasl.password': 'iAxAMDTL8atzac',
        'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
        'auto.offset.reset': 'earliest',
    }


    c = Consumer(conf)
    c.subscribe(['t_test'])

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                print("11111")
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())
                c.store_offsets(msg)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()