from kafka import KafkaProducer
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        """Initialize the Kafka producer with the given bootstrap servers."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'  # Wait for all replicas to acknowledge the message
        )
        self.topic = 'orders'

    def send_order(self, order_id, product, quantity, price):
        """Send an order message to Kafka."""
        try:
            order = {
                'order_id': order_id,
                'product': product,
                'quantity': quantity,
                'price': price,
                'timestamp': int(time.time() * 1000)
            }
            
            future = self.producer.send(self.topic, value=order)
            # Block until the message is sent (or timeout after 10 seconds)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Order sent successfully - Topic: {record_metadata.topic}, "
                       f"Partition: {record_metadata.partition}, "
                       f"Offset: {record_metadata.offset}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error sending order: {str(e)}")
            return False

    def close(self):
        """Close the producer connection."""
        self.producer.close()

def main():
    # Create producer instance
    producer = OrderProducer()
    
    try:
        # Send some sample orders
        for i in range(10):
            success = producer.send_order(
                order_id=f"ORDER_{i}",
                product=f"Product_{i}",
                quantity=i + 1,
                price=10.0 * (i + 1)
            )
            
            if success:
                logger.info(f"Successfully sent order {i}")
            else:
                logger.error(f"Failed to send order {i}")
            
            time.sleep(1)  # Wait 1 second between messages
            
    finally:
        # Always close the producer
        producer.close()

if __name__ == "__main__":
    main()
