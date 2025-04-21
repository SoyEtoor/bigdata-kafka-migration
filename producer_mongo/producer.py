from kafka import KafkaConsumer
import pymongo
import json
import os

# Configuración
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'transactions_mongodb'
MONGO_URI = 'mongodb://mongodb:27017/'
MONGO_DB = 'bigdata'
MONGO_COLLECTION = 'transactions_stats'

def create_mongo_connection():
    """Crea conexión a MongoDB"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]

def create_consumer():
    """Crea y retorna un consumidor Kafka"""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='mongodb-consumer-group'
    )

def process_messages(consumer, collection):
    """Procesa mensajes y los guarda en MongoDB"""
    for message in consumer:
        try:
            data = message.value
            print(f"Recibido: {data}")
            
            # Insertar en MongoDB
            collection.insert_one(data)
            print(f"Insertado en MongoDB: {data}")
            
        except Exception as e:
            print(f"Error procesando mensaje: {e}")

if __name__ == "__main__":
    print("Iniciando consumidor Kafka para MongoDB...")
    collection = create_mongo_connection()
    consumer = create_consumer()
    process_messages(consumer, collection)
