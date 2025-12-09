from confluent_kafka import Consumer, KafkaError
import json
import os
import time
import sys

# Configurazione base
# Legge la variabile d'ambiente definita nel docker-compose
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'to-alert-system'
GROUP_ID = 'alert_system_group'

consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

def wait_for_kafka():
    """
    Ciclo che tenta di connettersi a Kafka. 
    Blocca l'esecuzione finché Kafka non è pronto.
    """
    print(f"Tentativo di connessione a Kafka su: {BOOTSTRAP_SERVERS}...")
    while True:
        try:
            # Creiamo un consumer temporaneo per testare la connessione
            # Non serve sottoscriversi, basta istanziarlo e chiedere i metadata
            temp_consumer = Consumer(consumer_config)
            # list_topics è una chiamata di rete: se fallisce, Kafka è giù
            temp_consumer.list_topics(timeout=5.0)
            temp_consumer.close()
            print("--- KAFKA È PRONTO! Connessione stabilita. ---")
            return
        except Exception as e:
            print("Kafka non è ancora pronto... riprovo tra 5 secondi.")
            time.sleep(5)

class KafkaConsumerWrapper:
    def __init__(self, topic):
        self.consumer = Consumer(consumer_config)
        self.topic = topic
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        msg = self.consumer.poll(1.0)
        
        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            return None

        try:
            val_utf8 = msg.value().decode('utf-8')
            try:
                value = json.loads(val_utf8)
            except json.JSONDecodeError:
                value = val_utf8 # Ritorna la stringa grezza se non è JSON
                
            return value
        except Exception as e:
            print(f"Errore generico decodifica: {e}")
            return None

    def commit_offsets(self):
        self.consumer.commit()

    def close(self):
        self.consumer.close()

if __name__ == "__main__":
    wait_for_kafka()
    print("Inizializzazione Kafka Consumer Wrapper...")
    consumer = KafkaConsumerWrapper(TOPIC)
    
    print(f"Consumer avviato. In ascolto sul topic: '{TOPIC}'")
    
    try:
        while True:
            message = consumer.consume_messages()
            if message:
                print(f"--- MESSAGGIO RICEVUTO ---")
                print(message)
                print("--------------------------")
                consumer.commit_offsets()
    except KeyboardInterrupt:
        print("Arresto manuale ricevuto.")
    finally:
        consumer.close()