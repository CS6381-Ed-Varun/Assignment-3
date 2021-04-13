
from kazoo.client import KazooClient
from kazoo.client import KazooState
from random import randrange
from ansible.module_utils._text import to_bytes

import logging
import os
import sys
import time
import threading
import zmq


logging.basicConfig() # ignore

zoo = False
class Subscriber:
    def __init__(self, topic, flood, broker_add, new_port):
        self.broker = broker_add
        self.topic = topic
        self.flood = flood
        self.context = zmq.Context()
        self.sub = self.context.socket(zmq.SUB)
        self.path = '/leader/node'
        self.zk_object = KazooClient(hosts='127.0.0.1:2181') 
        self.zk_object.start()
        self.port = new_port

        if self.port:
            self.history_path = "/history/"
            self.history_node = self.history_path + "node"
            send_string = self.broker + "," + self.port
            if self.zk_object.exists(self.history_node):
                pass
            else:
                self.zk_object.ensure_path(self.history_path)
                self.zk_object.create(self.history_node, ephemeral = True)
            self.zk_object.set(self.history_node, send_string)
            self.connect_str = "tcp://" + self.broker + ":"+ self.port
            self.sub.connect(self.connect_str)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            global zoo
            zoo = True
        else:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event == None: 
                    data, stat = self.zk_object.get(self.path)
                    global zoo
                    zoo = True
            if zoo:  
                data, stat = self.zk_object.get(self.path) 
                data = str(data)
                address = data.split(",")
                self.connect_str = "tcp://" + self.broker + ":"+ address[1][:-1]
                self.sub.connect(self.connect_str)
                self.sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            else:
                print ("Zookeeper hasn't started")

        if self.flood:
            for i in range(1,10):
                port = str(5558+i)
                self.sub.connect("tcp://127.0.0.1:" + port)


    def run(self):
        while True:
            @self.zk_object.DataWatch(self.path)
            def watch_node(data, stat, event):
                if event != None:
                        print(event.type)
                        if event.type == "CHANGED": 
                            self.sub.close()
                            self.context.term()
                            time.sleep(1)
                            self.context = zmq.Context()
                            self.sub = self.context.socket(zmq.PUB)
                            data, stat = self.zk_object.get(self.path) 
                            address = data.split(",")
                            self.connect_str = "tcp://" + self.broker + ":"+ address[1]
                            print(self.connect_str)
                            self.sub.connect(self.connect_str)

            string = self.sub.recv_string()
            topic, price, ownership, history, pub_time = string.split()
            pub_time = float(pub_time)
            time_diff = time.time() - pub_time
            print ("The time difference is: ", time_diff)
            print (topic + " : " + price)
            with open("./results/latency_{}.csv".format(topic), "a") as f:
                f.write(str(time_diff) + "\n")

    def close(self):
        self.sub.close(0)

if __name__ == '__main__':
    topic = "MSFT"
    broker = "127.0.0.1"
    port =  ""
    print ('Starting Subscriber with Topic:',topic)
    sub = Subscriber(topic, False, broker, port)

    if zoo:
        sub.run()

