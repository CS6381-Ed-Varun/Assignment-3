 
import sys
import os
import zmq
from threading import Thread
import random
from ansible.module_utils._text import to_bytes
import time
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

#logging required by zookeeper -ignore
logging.basicConfig()


class Publisher:
    def __init__(self, topic, flood, broker_add, ownership_strength):
        self.broker = broker_add
        self.strength = ownership_strength
        self.topic = topic
        self.context = zmq.Context()
        self.pub = self.context.socket(zmq.PUB)

        self.path = '/leader/node'
        self.zk_object = KazooClient(hosts='127.0.0.1:2181') 
        self.zk_object.start()

        data, stat = self.zk_object.get(self.path)
        data = str(data)
        address = data.split(",")
        connect_str = "tcp://" + self.broker + ":"+ address[0][2:]
        print("Pub: " + connect_str +", " + self.topic)
        self.pub.connect(connect_str)
        

    def run(self):
        history = 3
        while True:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event != None:
                        print(event.type)
                        if event.type == "CHANGED": 
                            self.pub.close()
                            self.context.term()
                            time.sleep(2)
                            self.context = zmq.Context()
                            self.pub = self.context.socket(zmq.PUB)

                            data, stat = self.zk_object.get(self.path) 
                            address = data.split(",")
                            self.connect_str = "tcp://" + self.broker + ":"+ address[0]
                            print(self.connect_str)
                            self.pub.connect(self.connect_str)

            price = str(random.randrange(20, 60))
            pub_timestamp = time.time()
            self.pub.send_string("%s %s %i %i %i" % (self.topic, price, self.strength, history, pub_timestamp))
            time.sleep(1) 

    def close(self):
        self.pub.close(0)

if __name__ == '__main__':
    topic = "MSFT"
    broker = "127.0.0.1"
    pub4 = Publisher("AAPL",False,broker,1)
    pub4.run()
