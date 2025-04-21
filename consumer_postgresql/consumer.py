from kafka import KafkaConsumer
import psycopg2
import json
from psycopg2 import sql

# Configuración
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'transactions_postgresql'
POSTGRES_CONFIG = {
    'host': 'postgres',
    'database': 'bigdata',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

def create_postgres_connection():
    """Crea conexión a PostgreSQL"""
    return psycopg2.connect(**POSTGRES_CONFIG)

def create_consumer():
    """Crea y retorna un consumidor Kafka"""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='postgresql-consumer-group'
    )

def ensure_table_exists(conn):
    """Asegura que la tabla exista en PostgreSQL"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS location_stats (
        id SERIAL PRIMARY KEY,
        location VARCHAR(255),
        transaction_count INTEGER,
        avg_value NUMERIC(15, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    conn.commit()

def process_messages(consumer, conn):
    """Procesa mensajes y los guarda en PostgreSQL"""
    ensure_table_exists(conn)
    
    for message in consumer:
        try:
            data = message.value
            print(f"Recibido: {data}")
            
            # Insertar en PostgreSQL
            with conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO location_stats (location, transaction_count, avg_value)
                    VALUES (%s, %s, %s)
                """)
                cursor.execute(insert_query, (
                    data.get('location'),
                    data.get('transaction_count'),
                    data.get('avg_value')
                ))
            conn.commit()
            print(f"Insertado en PostgreSQL: {data}")
            
        except Exception as e:
            print(f"Error procesando mensaje: {e}")
            conn.rollback()

if __name__ == "__main__":
    print("Iniciando consumidor Kafka para PostgreSQL...")
    conn = create_postgres_connection()
    consumer = create_consumer()
    process_messages(consumer, conn)
