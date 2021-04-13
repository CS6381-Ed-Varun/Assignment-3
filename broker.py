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

logging.basicConfig()  # ignore


class broker:
    def __init__(self):
        #initializing
        self.joined = True
        self.context = zmq.Context()
        self.backend = self.context.socket(zmq.XSUB)
        self.frontend = self.context.socket(zmq.XPUB)
        #switched from proxy to actually sending to handle ownership + history
        self.frontend.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.frontend.send_multipart([b'\x01', b'10001'])
        
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)
        #setting up seperate port + ip to send history
        self.sub_url = 0
        self.sub_port = 0
        self.newSub = False
        #lists for history + to track topics for ownership
        self.full_data_queue = []
        self.topicInd = 0
        self.tickers = []
        #intialize zk
        self.zk_object = KazooClient(hosts='127.0.0.1:2181')
        self.zk_object.start()
        self.path = '/home/'
        #set-up znodes for different brokers
        znode1 = self.path + "broker1"
        if self.zk_object.exists(znode1):
            pass
        else:
            #ensure path if it's not there, create
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode1, to_bytes('5555, 5556'))
        #set-up znodes for different brokers
        znode2 = self.path + "broker2"
        if self.zk_object.exists(znode2):
            pass
        else:
            #ensure path if it's not there, create
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode2, to_bytes('5557, 5558'))
        #set up znode for brokers
        znode3 = self.path + "broker3"
        if self.zk_object.exists(znode3):
            pass
        else:
            #ensure path if it's not there, create
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode3, to_bytes('5553, 5554'))
        #leader election
        self.election = self.zk_object.Election(self.path, "leader")
        leader_list = self.election.contenders()
        self.leader = leader_list[-1]
        #get ports from the leader
        address = self.leader.split(",")
        pub_addr = "tcp://*:" + address[0]
        sub_addr = "tcp://*:" + address[1]
        print("Current elected broker: ", pub_addr + "," + sub_addr)
        self.backend.bind(pub_addr)
        self.frontend.bind(sub_addr)
        
        self.watch_dir = self.path + self.leader
        #set-up path + znode
        self.leader_path = "/leader/"
        self.leader_node = self.leader_path + "node"
        if self.zk_object.exists(self.leader_node):
            pass
        else:
            self.zk_object.ensure_path(self.leader_path)
            self.zk_object.create(self.leader_node, ephemeral=True)
        #allow pubs/subs to find ports through zk
        self.zk_object.set(self.leader_node, to_bytes(self.leader))
        #make a history node
        self.history_node = '/history/node'
        #set-up and start threads
        self.threading1 = threading.Thread(target=self.new_sub)
        self.threading1.daemon = True
        self.threading1.start()
    #set_up a new sub and sends it the history
    def new_sub(self):
        while True:
                @self.zk_object.DataWatch(self.history_node)
                def watch_node(data, stat, event):
                    if event == None:
                        data, stat = self.zk_object.get(self.history_node)
                        print("Get a new subscriber here")
                        address = data.split(",")
                        pub_url = "tcp://" + address[0] + ":" + address[1]
                        self.sub_port = address[1]
                        self.sub_url = pub_url
                        self.newSub = True
    #creates and maintains history 
    def history_(self, hist_list, ind, history, msg):
        if len(hist_list[ind]) < history:
            hist_list[ind].append(msg)
        else:
            hist_list[ind].pop(0)
            hist_list[ind].append(msg)
        return hist_list
    #recieving data from pubs + breaking it into useful components for sending
    def send(self):
        while self.joined = True:
            events = dict(self.poller.poll(5000))  # number is milliseconds 
            if self.backend in events:
                msg = self.backend.recv_multipart()
                print(msg)
                content = msg[0]
                content = str(content)
                topic, price, strength, history, pub_time = content.split(" ")

                if topic not in self.tickers:
                    self.tickers.append(topic)
                    cur_strength = 0
                    prior_strength = 0
                    count = 0
                    history_vec = []
                    strength_list = []
                    topic_index = 0
                    prior_message = []
                    message = []

                    full_data = [cur_strength, prior_strength, count, history_vec, strength_list, topic_index,
                                 prior_message, message]
                    self.full_data_queue.append(full_data)
                    # collect the msg for the new topic
                    topic_msg, histry_msg, strength, strength_list = self.schedule(self.full_data_queue[self.topicInd], msg)
                    self.topicInd += 1
                else:
                    topic_ind = self.tickers.index(topic)
                    topic_msg, histry_msg, strength, strength_list = self.schedule(self.full_data_queue[topic_ind], msg)
                #bind a new pub to a new sub and send it the history
                if self.newSub:
                    ctx = zmq.Context()
                    pub = ctx.socket(zmq.PUB)
                    pub.bind(self.sub_url)
                    if strength == max(strength_list):
                        curInd = strength_list.index(strength)
                        time.sleep(1)
                        for i in range(len(histry_msg)):
                            pub.send_multipart(histry_msg[i])
                            time.sleep(0.1)
                    pub.unbind(self.sub_url)
                    pub.close()
                    ctx.term()
                    url = "tcp://*:" + self.sub_port
                    self.frontend.bind(url)
                    self.newSub = False #set sub back to false once history if completed
                    print("History sent")
                #if not new, send like normal
                else:
                    self.frontend.send_multipart(topic_msg)

            if self.frontend in events:
                msg = self.frontend.recv_multipart()
                self.backend.send_multipart(msg)

        def schedule(self, info, msg):
            [cur_strength, prior_strength, count, history_vec, strength_list, topic_index, prior_message, message] = info

            sample_num = 10 #arbitrary 
            content = msg[0]
            content = str(content) #cast for split
            topic, price, strength, history, pub_time = content.split(" ")

            strength = int(strength)
            history = int(history)
            # create the history for each stock
            if strength not in strength_list:
                strength_list.append(strength)
                # create list for this stock/pub
                history_vec.append([])
                history_vec = self.history_(history_vec, topic_index, history, msg)
                topic_index += 1 
            else:
                curInd = strength_list.index(strength)
                history_vec = self.history_(history_vec, curInd, history, msg)

            # strength check to see if pub should take over a given stock's publishing
            if strength > cur_strength:
                prior_strength = cur_strength
                cur_strength = strength
                prior_message = message
                message = msg
                count = 0
                print("new strongest ownership publisher")
            elif strength == cur_strength:
                message = msg
                count = 0
            else:
                count = count + 1
                if count >= sample_num:
                    cur_strength = prior_strength
                    message = prior_message
                    count = 0

            info[0] = cur_strength
            info[1] = prior_strength
            info[2] = count
            info[3] = history_vec
            info[4] = strength_list
            info[5] = topic_index
            info[6] = prior_message
            info[7] = message

            histInd = strength_list.index(cur_strength)
            histry_msg = history_vec[histInd]

            return message, histry_msg, strength, strength_list

    def monitor(self):
        while True:
            @self.zk_object.DataWatch(self.watch_dir)
            def watch_node(data, stat, event):
                if event != None:
                    print(event.type)
                    if event.type == "DELETED":  # redo a election
                        self.election = self.zk_object.Election(self.path, "leader")
                        leader_list = self.election.contenders()
                        self.leader = leader_list[-1].encode('latin-1')

                        self.zk_object.set(self.leader_node, self.leader)

                        self.backend.unbind(self.current_pub)
                        self.frontend.unbind(self.current_sub)

                        address = self.leader.split(",")
                        pub_addr = "tcp://*:" + address[0]
                        sub_addr = "tcp://*:" + address[1]

                        self.backend.bind(pub_addr)
                        self.frontend.bind(sub_addr)

            self.election.run(self.send)

    def close(self):
        self.joined = False


if __name__ == '__main__':
    broker = broker()
    broker.monitor()
