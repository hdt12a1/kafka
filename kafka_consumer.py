from kafka import KafkaConsumer
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderConsumer:
    def __init__(self, consumer_id, group_id='order_processing_group', 
                 bootstrap_servers=['localhost:9092']):
        """Initialize the Kafka consumer with the given consumer_id and group_id."""
        self.consumer_id = consumer_id
        self.consumer = KafkaConsumer(
            'orders',  # topic name
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start reading from the beginning if no offset is stored
            enable_auto_commit=True,  # Automatically commit offsets
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # Increase max poll interval for better handling of processing time
            max_poll_interval_ms=300000,  # 5 minutes
            # Configure heartbeat for faster rebalancing
            heartbeat_interval_ms=3000,
            session_timeout_ms=30000
        )

    def process_order(self, order):
        """Process a single order. In a real application, this would contain business logic."""
        logger.info(f"Consumer {self.consumer_id} processing order: {order}")
        # Simulate some processing time
        import time
        time.sleep(1)

    def start_consuming(self):
        """Start consuming messages from Kafka."""
        try:
            logger.info(f"Consumer {self.consumer_id} started in consumer group")
            
            # Print assigned partitions
            for tp in self.consumer.assignment():
                logger.info(f"Consumer {self.consumer_id} assigned to: "
                          f"Topic: {tp.topic}, Partition: {tp.partition}")

            for message in self.consumer:
                try:
                    order = message.value
                    logger.info(f"Consumer {self.consumer_id} received message from "
                              f"partition {message.partition} @ offset {message.offset}")
                    
                    # Process the order
                    self.process_order(order)
                    
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Consumer {self.consumer_id} encountered an error: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Close the consumer connection."""
        self.consumer.close()
        logger.info(f"Consumer {self.consumer_id} closed")

def start_consumer(consumer_id):
    """Helper function to start a consumer instance."""
    consumer = OrderConsumer(consumer_id)
    consumer.start_consuming()

def main():
    # Number of consumers to start (should be <= number of partitions)
    num_consumers = 3
    
    # Start multiple consumers using a thread pool
    with ThreadPoolExecutor(max_workers=num_consumers) as executor:
        for i in range(num_consumers):
            executor.submit(start_consumer, f"Consumer_{i}")

if __name__ == "__main__":
    main()
