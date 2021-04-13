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

        self.context = zmq.Context()
        self.backend = self.context.socket(zmq.XSUB)
        self.frontend = self.context.socket(zmq.XPUB)
        self.frontend.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.frontend.send_multipart([b'\x01', b'10001'])

        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)

        self.sub_url = 0
        self.sub_port = 0
        self.newSub = False

        self.full_data_queue = []
        self.topicInd = 0
        self.tickers = []

        self.zk_object = KazooClient(hosts='127.0.0.1:2181')
        self.zk_object.start()
        self.path = '/home/'

        znode1 = self.path + "broker1"
        if self.zk_object.exists(znode1):
            pass
        else:
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode1, '5555,5556')

        znode2 = self.path + "broker2"
        if self.zk_object.exists(znode2):
            pass
        else:
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode2, '5557,5558')

        znode3 = self.path + "broker3"
        if self.zk_object.exists(znode3):
            pass
        else:
            self.zk_object.ensure_path(self.path)
            self.zk_object.create(znode3, '5553,5554')

        self.election = self.zk_object.Election(self.path, "leader")
        leader_list = self.election.contenders()
        self.leader = leader_list[-1]

        address = self.leader.split(",")
        pub_addr = "tcp://*:" + address[0]
        sub_addr = "tcp://*:" + address[1]
        print("Current elected broker: ", pub_addr + "," + sub_addr)
        self.backend.bind(pub_addr)
        self.frontend.bind(sub_addr)

        self.watch_dir = self.path + self.leader

        self.leader_path = "/leader/"
        self.leader_node = self.leader_path + "node"
        if self.zk_object.exists(self.leader_node):
            pass
        else:
            self.zk_object.ensure_path(self.leader_path)
            self.zk_object.create(self.leader_node, ephemeral=True)
        self.zk_object.set(self.leader_node, to_bytes(self.leader))

        self.history_node = '/history/node'

        self.threading1 = threading.Thread(target=self.new_sub)
        self.threading1.daemon = True
        self.threading1.start()

    def new_sub(self):
        print("x key to send history")
        while True:
            addr_input = input()
            if addr_input == "x":
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

    def history_(self, hist_list, ind, history, msg):
        if len(hist_list[ind]) < history:
            hist_list[ind].append(msg)
        else:
            hist_list[ind].pop(0)
            hist_list[ind].append(msg)
        return hist_list

    def send(self):
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
                # start to collect the msg for the new topic
                topic_msg, histry_msg, strength, strength_list = self.schedule(self.full_data_queue[self.topicInd], msg)
                self.topicInd += 1
            else:
                topic_ind = self.tickers.index(topic)
                topic_msg, histry_msg, strength, strength_list = self.schedule(self.full_data_queue[topic_ind], msg)

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
                self.newSub = False
                print("History sent")
            else:
                self.frontend.send_multipart(topic_msg)

        if self.frontend in events:
            msg = self.frontend.recv_multipart()
            self.backend.send_multipart(msg)

    def schedule(self, info, msg):
        [cur_strength, prior_strength, count, history_vec, strength_list, topic_index, prior_message, message] = info

        sample_num = 10
        content = msg[0]
        content = str(content)
        topic, price, strength, history, pub_time = content.split(" ")

        strength = int(strength)
        history = int(history)
        # creat the history stock for each publisher, should be FIFO
        if strength not in strength_list:
            strength_list.append(strength)
            # create list for this publisher
            history_vec.append([])
            history_vec = self.history_(history_vec, topic_index, history, msg)
            topic_index += 1  # the actual size of the publishers
        else:
            curInd = strength_list.index(strength)
            history_vec = self.history_(history_vec, curInd, history, msg)

        # get the highest strength msg to register the hash ring, using a heartbeat listener
        if strength > cur_strength:
            prior_strength = cur_strength
            cur_strength = strength
            prior_message = message
            message = msg
            count = 0
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
        self.backend.close(0)
        self.frontend.close(0)


if __name__ == '__main__':
    broker = broker()
    broker.monitor()
