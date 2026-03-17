from confluent_kafka import Consumer,TopicPartiton
import json

class InvoiceConsumer:
    
    def __init__(self):
        self.conf = {
            "bootstrap.servers" : "localhost:9092",
            "group.id" : "DemoGroup",
            "auto.offset.reset" : "earliest"
        }

    def invoice_consumer(self , consumer):

        while True:
            msg = consumer.poll(timeout = 1)

            if msg is None :
                continue

            if msg.error():
                print(f"Erorr {msg.error()}")
                continue 
            
            print(f"Key {msg.key().decode('utf-8')}" )
            print(f"Value {json.loads(msg.value().decode('utf'))['InvoiceNumber']}")

    def main(self):
        consumer = Consumer(self.conf)
        consumer.subscribe(['invoices'])

        self.invoice_consumer(consumer)

if __name__ =='__main__':
    invoiceconsumer = InvoiceConsumer()
    invoiceconsumer.main()

#msgs = consumer.consume(num_messages=10, timeout=1.0) -> Batch fetch
#consumer.assign([TopicPartition('my_topic', 0)])  -> Manual partition assignment , No group coordination
#consumer.seek(TopicPartition('my_topic', 0, 5))   -> Jump to specific offset

