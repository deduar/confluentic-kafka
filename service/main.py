import os
import uuid
from uuid import uuid4
from decimal import *

# import confluent_kafka.admin, pprint
# from confluent_kafka.admin import AdminClient, NewTopic

from confluent_kafka import KafkaError, KafkaException

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from service.cfp import CFP
from service.mov import Mov

def dict_to_cfp(obj,ctx):
    if obj is None:
        return None
    return CFP(movimientoId=obj['movimientoId'],
                productoId=obj['productoId'],
                fechaOperacion=obj['fechaOperacion'],
                co2=obj['co2'],
                ch4=obj['ch4'],
                n2o=obj['n2o'],
                h2o=obj['h2o'],
                associated_plastic=obj['associated_plastic']
                )

def cfp_to_dict(cfp, ctx):
    return dict(movimientoId=cfp.movimientoId,
                productoId=cfp.productoId,
                fechaOperacion=cfp.fechaOperacion,
                co2=cfp.co2,
                ch4=cfp.ch4,
                n2o=cfp.n2o,
                h2o=cfp.h2o,
                associated_plastic=cfp.associated_plastic)

def dict_to_mov(obj,ctx):
    if obj is None:
        return None
    return Mov(movimientoId=obj['movimientoId'],
                productoId=obj['productoId'],
                fechaOperacion=obj['fechaOperacion'],
                importe=obj['importe'],
                concepto=obj['concepto'],
                descripcionOperacion=obj['descripcionOperacion'],
                categoriaId=obj['categoriaId'],
                marcaComercial=obj['marcaComercial']
                )

def mov_to_dict(mov, ctx):
    return dict(movimientoId=mov.movimientoId,
                productoId=mov.productoId,
                fechaOperacion=mov.fechaOperacion,
                importe=mov.importe,
                concepto=mov.concepto,
                descripcionOperacion=mov.descripcionOperacion,
                categoriaId=mov.categoriaId,
                marcaComercial=mov.marcaComercial)

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for CFP record {}: {}".format(msg.key(), err))
        return
    print('CFP record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main():
    print("main from KAFKA")

    def error_cb(err):
        print("Client error: {}".format(err))
        if err.code() == KafkaError._ALL_BROKERS_DOWN or \
        err.code() == KafkaError._AUTHENTICATION:
            raise KafkaException(err)
        
    def acked(err, msg):
        if err is not None:
            print('Failed to deliver message: {}'.format(err.str()))
        else:
            print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))

    #
    #CREATE TOPICS
    # admin_client = AdminClient({
    #     'bootstrap.servers': 'b-3.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-2.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096',
    #     'sasl.mechanism': 'SCRAM-SHA-512',
    #     'security.protocol': 'SASL_SSL',
    #     'sasl.username': 'coinscrap',
    #     'sasl.password': 'iAxAMDTL8atzac'
    #     # 'bootstrap.servers':'docker_kafka-kafka-broker-1-1:9092'
    # })

    # topic_list = []
    # topic_list.append(NewTopic(topic_producer,1,3))
    
    # pprint.pprint(admin_client.list_topics().topics)
    # admin_client.delete_topics(topic_list)
    # print("-----------")
    # admin_client.create_topics(topic_list)
    # pprint.pprint(admin_client.list_topics().topics)


    #
    ##Schema
    print("Schema -------------------")
    topic_consumer = "t_raw"
    topic_producer = "t_cfp"

    schema_producer = "zpfm.coinscrap.movimientoshuellacarbono.avsc"
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema_producer}") as f:
        schema_str_producer = f.read()
    sr_conf = {'url': 'http://docker_kafka-schema-registry-1:8081'}
    schema_registry_client_producer = SchemaRegistryClient(sr_conf)

    avro_deserializer_producer = AvroDeserializer(schema_registry_client_producer,
                                         schema_str_producer,
                                         dict_to_cfp)

    avro_serializer_producer = AvroSerializer(schema_registry_client_producer,
                                     schema_str_producer,
                                     cfp_to_dict)


    schema_consumer = "zpfm.coinscrap.movimientoscategorizados.avsc"
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/avro/{schema_consumer}") as f:
        schema_str_consumer = f.read()
    sr_conf = {'url': 'http://docker_kafka-schema-registry-1:8081'}
    schema_registry_client_consumer = SchemaRegistryClient(sr_conf)

    avro_deserializer_consumer = AvroDeserializer(schema_registry_client_consumer,
                                         schema_str_consumer,
                                         dict_to_mov)

    avro_serializer_consumer = AvroSerializer(schema_registry_client_consumer,
                                     schema_str_consumer,
                                     mov_to_dict)

    string_serializer = StringSerializer('utf_8')

    # #
    # ##Create producer
    print("Produccer -------------------")
    p = Producer({
        'bootstrap.servers': 'b-3.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-2.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'coinscrap',
        'sasl.password': 'iAxAMDTL8atzac',
        # 'bootstrap.servers':'docker_kafka-kafka-broker-1-1:9092',
        'error_cb': error_cb,
    })

    print("Produccer CFP -------------------")
    p.poll(0.0)
    try:
        cfp = CFP(
            movimientoId="Test123",
            productoId="TDC",
            fechaOperacion=1694514499406,
            co2=1,
            ch4=2,
            n2o=3,
            h2o=4,
            associated_plastic=5)
        p.produce(topic=topic_producer,
                key=string_serializer(str(uuid4())),
                value=avro_serializer_producer(cfp, SerializationContext(topic_producer, MessageField.VALUE)),
                on_delivery=delivery_report)
    except ValueError:
        print("Invalid input, discarding record...")

    print("\nFlushing records...")
    p.flush()

    print("Produccer MOV -------------------")
    p.poll(0.0)
    try:
        mov = Mov(
            movimientoId="Test123",
            productoId="TDC",
            fechaOperacion=1694514499406,
            importe=Decimal("-10.3"),
            concepto="UNDOS TRES",
            descripcionOperacion="CUATRO CINCO",
            categoriaId=77,
            marcaComercial="SEIS SIETE")
        p.produce(topic=topic_consumer,
                key=string_serializer(str(uuid4())),
                value=avro_serializer_consumer(mov, SerializationContext(topic_consumer, MessageField.VALUE)),
                on_delivery=delivery_report)
    except ValueError:
        print("Invalid input, discarding record...")

    print("\nFlushing records...")
    p.flush()


    #
    ##Create consumer
    print("Consumer -------------------") 
    c = Consumer({
        'bootstrap.servers':'b-3.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-1.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096,b-2.abacfpdevserverless.z5azq9.c4.kafka.eu-west-3.amazonaws.com:9096',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'security.protocol': 'SASL_SSL',
        'sasl.username': 'coinscrap',
        'sasl.password': 'iAxAMDTL8atzac',
        # 'bootstrap.servers':'docker_kafka-kafka-broker-1-1:9092',
        'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
        'auto.offset.reset': 'earliest',
        'enable.auto.commit':False
        })

    # print("Consumer CFP -------------------") 
    # c.subscribe([topic_producer])
    # while True:
    #     try:
    #         # SIGINT can't be handled when polling, limit timeout to 1 second.
    #         msg = c.poll()
    #         if msg is None:
    #             continue

    #         user = avro_deserializer_producer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    #         if user is not None:
    #              print("CFP record {}: movimientoId: {} productoId: {} fechaOperacion: {} co2: {} ch4: {} n2o: {} h2o: {} associated_plastic: {}"
    #                   .format(msg.key(), user.movimientoId,
    #                           user.productoId,
    #                           user.fechaOperacion,
    #                           user.co2,
    #                           user.ch4,
    #                           user.n2o,
    #                           user.h2o,
    #                           user.associated_plastic
    #                           ))
    #     except KeyboardInterrupt:
    #         break

    print("Consumer MOV -------------------") 
    c.subscribe([topic_consumer])
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = c.poll()
            if msg is None:
                continue

            mov = avro_deserializer_consumer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if mov is not None:
                print("MOV record {}: movimientoId: {} productoId: {} fechaOperacion: {} importe: {} concepto: {} descripcionOperacion: {} categoriaId: {} marcaComercial: {}"
                      .format(msg.key(), mov.movimientoId,
                              mov.productoId,
                              mov.fechaOperacion,
                              mov.importe,
                              mov.concepto,
                              mov.descripcionOperacion,
                              mov.categoriaId,
                              mov.marcaComercial
                              ))
                print("----- MAKE ESTIMATE OF CFP")
                print("----- Publish ESTIMATE CFP topic PRODUCER (CFP)")
                print("----- Agregate to MongoDB")
                print()
        except KeyboardInterrupt:
            break
    c.close()
