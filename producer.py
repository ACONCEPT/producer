from postgreslib.database_connection import DBConnection
from helpers.kafka import KafkaWriter, get_topic
import json

class IngestionProducer(KafkaWriter):
    def __init__(self,bootstrap_servers,datasource,number = False):
        super().__init__(bootstrap_servers)
        print("starting injestion producer for number limit :  {}".format(number))
        self.datasource = datasource
        self.db = DBConnection(datasource)
        self.number = number

    def publish_to_topic(self, datasource, table, data):
        self.jsonproducer.send(topic,json.dumps(data))
        self.jsonproducer.flush()

    def get_ingestion_data(self,table):
        self.data , self.header = self.db.stream_table(table)

    def ingest_data(self,table):
        self.get_ingestion_data(table)
        generator,header = self.db.stream_table("sales_orders")
        data = {}
        data["meta"] = {"table":table}
        topic = get_topic(self.datasource,table)
        print("streaming data from table {} to topic {}".format(table,topic))
        x = 0
        stat = {}
#        stat["topic"] = topic
        for i,record in enumerate(generator):
            data["record"] = {str(h.name):str(v) for h,v in zip(header,record)}
            self.produce(data,topic)
#            self.jsonproducer.send(topic,json.dumps(data))
            x = i
#            if x % 1000 == 0:
#                stat["count"] = x
#                self.produce_stats(json.dumps(stat))
            if self.number:
                if x >= self.number:
                    break

        self.produce_debug("completed producing {}, {} records".format(table,x))
#        self.produce_stats(stat)

def main(bootstrap_servers,db,table):
    print("main table {}".format(table))
    producer = IngestionProducer(bootstrap_servers,db)
    producer.ingest_data(table)

if __name__ == '__main__':
    pass
