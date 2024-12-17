from kafka import KafkaProducer, KafkaConsumer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka SASL configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'ixu_hela',
    'sasl_plain_password': 'ixu_hela'
}

def test_producer():
    """Test producer authentication"""
    try:
        producer = KafkaProducer(
            **KAFKA_CONFIG,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Try to send a test message
        message = {"test": "authentication"}
        future = producer.send('ixu.rule.reference.response', message)
        result = future.get(timeout=10)
        
        logger.info("Producer test successful!")
        logger.info(f"Message sent to partition {result.partition} at offset {result.offset}")
        
    except Exception as e:
        logger.error(f"Producer test failed: {str(e)}")
    finally:
        producer.close()

def test_consumer():
    """Test consumer authentication"""
    try:
        consumer = KafkaConsumer(
            'ixu.rule.reference.response',
            group_id='ixu_hela.group',
            **KAFKA_CONFIG,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        
        logger.info("Consumer connected successfully!")
        
        # Try to read messages for 10 seconds
        import time
        start_time = time.time()
        while time.time() - start_time < 10:
            msg_pack = consumer.poll(timeout_ms=1000)
            if msg_pack:
                for tp, messages in msg_pack.items():
                    for message in messages:
                        logger.info(f"Received: {message.value}")
        
        logger.info("Consumer test successful!")
        
    except Exception as e:
        logger.error(f"Consumer test failed: {str(e)}")
    finally:
        consumer.close()

if __name__ == "__main__":
    logger.info("Testing Producer Authentication...")
    test_producer()
    
    logger.info("\nTesting Consumer Authentication...")
    test_consumer()
