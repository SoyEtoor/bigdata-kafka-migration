from kafka import KafkaProducer
import json
import time
import requests

# Configuración
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'transactions_postgresql'
RESULTS_URL = 'https://raw.githubusercontent.com/SoyEtoor/spark-labs-parcial/main/results/location_statistics/data.jsonl'

def fetch_data():
    """Obtiene datos del repositorio GitHub"""
    response = requests.get(RESULTS_URL)
    if response.status_code == 200:
        return [json.loads(line) for line in response.text.split('\n') if line]
    return []

def create_producer():
    """Crea y retorna un productor Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def produce_messages(producer, data):
    """Envía mensajes al topic de Kafka"""
    for record in data:
        try:
            producer.send(TOPIC_NAME, value=record)
            print(f"Enviado a PostgreSQL topic: {record}")
            time.sleep(1)  # Para no saturar
        except Exception as e:
            print(f"Error enviando mensaje: {e}")

if __name__ == "__main__":
    print("Iniciando productor Kafka para PostgreSQL...")
    producer = create_producer()
    
    while True:
        data = fetch_data()
        if data:
            produce_messages(producer, data)
        else:
            print("No se pudo obtener datos o no hay datos nuevos")
        
        time.sleep(60)  # Espera 1 minuto antes de verificar nuevos datos
