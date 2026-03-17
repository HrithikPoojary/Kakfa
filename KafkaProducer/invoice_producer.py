from confluent_kafka import Producer
import json
import time 

class InvoiceProducer:
    
    def __init__(self):
        self.data_path = '/home/hrithik_poojary/data/json/invoices.json'
        self.topic = "invoices"
        self.conf = {
            "bootstrap.servers":"localhost:9092",
           # "security.protocol":"SASL_SSL",
           # "sasl.mechanism" : "PLAIN",
           # "sasl.username" : "<api_key>",
           # "sasl.password":'<api_secret>',
            "client.id" : "hrithik-laptop"
        }

    def delivery_callback(self , err , msg):               # err  - Error (binary format)
        if err:                                            # msg - we will recieve object which contains all infromation regarding the message (binary format)
            print(f"Error : Message failed deleivery")     # topics , partition , offset , key , value etc ...
        else :
            key = msg.key().decode('utf-8')
            invoice_id = json.loads(msg.value().decode('utf-8'))['InvoiceNumber']
            print(f"Produce event : key {key} value = {invoice_id}")

    
    def produce_invoices(self , producer ,counts):

        counter = 0
        with open(self.data_path , mode = 'r') as file:
            for line in file:                            # line - value
                invoice = json.loads(line)                # json object
                store_id = invoice["StoreID"]             # key 
                                                          # producer will automatically pass the timestap

                producer.produce(self.topic , 
                                    key = store_id , 
                                    value = line ,
                                    callback = self.delivery_callback)      # this will converted to binary format and send to broker/cluster
                time.sleep(0.5)                                             # just to see the output properly (not recommended)
                producer.poll(timeout = 1)
                # produce only first 10 records 
                counter = counter + 1
                if counter == counts:
                    break                                                             # 1 -second(timeout) -> this will pull the acknowlegdement from the Producer which will come from broker
                                                                                      # poll method will make sure that delivery call back is called for each message.
                                                                                      # recommended -> produce 100 msg then poll , we will all the acknownledgement get once
                                                                                      #              -> try for next 100 msgs and poll

                # Asynchronous Process
                # it will keep on producing the messages it will not case about acknowlegdement call back 
    
    def start(self):
        kakfa_producer = Producer(self.conf)
        self.produce_invoices(kakfa_producer , 10)
        kakfa_producer.flush(timeout = 10 )   # wait for acknownledgement before terminating the application


if __name__=='__main__':
    invoice_producer = InvoiceProducer()
    invoice_producer.start()
     