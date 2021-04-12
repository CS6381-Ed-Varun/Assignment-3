
import zmq
import os
import sys
import time
import threading
import zmq
from random import randrange
from ansible.module_utils._text import to_bytes
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

logging.basicConfig()

class broker:

	def __init__(self):
		#setting up proxy sockets
		self.context = zmq.Context()
		self.frontend = self.context.socket(zmq.XPUB)
		self.backend = self.context.socket(zmq.XSUB)

        self.sub_port = 0
        self.newSub = False 

		#Connecting to zookeeper - 2181 from config, ip should/can be changed
		self.zk_object = KazooClient(hosts='127.0.0.1:2181')
		self.zk_object.start()
		self.path = '/home/'

		#creating a znode + path for each broker. We need these for elections and to monitor if it becomes leader
		node1 = self.path + "broker1"
		if self.zk_object.exists(node1):
			pass
		else:
			#create the file path since zookeeper is file structured. 
			self.zk_object.ensure_path(self.path)
			#create the znode with port info for the pubs + subs to use to find the broker sockets. This is 'data' field used in pub + sub
			self.zk_object.create(node1, to_bytes("5555,5556"))

		#znode2 (same w/ modified port #'s')
		node2 = self.path + "broker2"
		if self.zk_object.exists(node2):
			pass
		else:
			#make sure the path exists
			self.zk_object.ensure_path(self.path)
			#create the znode with port info for the pubs + subs to use in addr[]
			self.zk_object.create(node2, to_bytes("5557,5558"))

		#znode 3 (same as above 2/ modified ports)
		node3 = self.path + "broker3"
		if self.zk_object.exists(node3):
			pass
		else:
			#make sure the path exists
			self.zk_object.ensure_path(self.path)
			#create the znode with port info for the pubs + subs to use in addr[]
			self.zk_object.create(node3, to_bytes("5559,5560"))

		#Select a leader for the first time
		self.election = self.zk_object.Election(self.path, "leader")    #requirements is the '/home/' (self.path) location in hierarchy, named leader
		potential_leaders = self.election.contenders() #create a list of broker znodes 
		self.leader = str(potential_leaders[-1]) #always select last one (arbitrary but simple process)
		print("Leader ports: " + self.leader) 

		#use port #'s from the leader to finish connecting the proxy'
		addr = self.leader.split(",") 
&&&&		self.frontend.bind("tcp://127.0.0.1:" + addr[0])  #will want to modify ip as usual
&&&&		self.backend.bind("tcp://127.0.0.1:" + addr[1])

		#set-up znode for the newly minted leader
		self.watch_dir = self.path + self.leader 
		self.leader_path ="/leader/"   
		self.leader_node = self.leader_path + "node"   #saving the path in zookeeper hierarchy to a var
		if self.zk_object.exists(self.leader_node):
			pass
		#if the path doesn't exist -> make it and populate it with a znode
		else:
			self.zk_object.ensure_path(self.leader_path)
			self.zk_object.create(self.leader_node, ephemeral = True) #ephemeral so it disappears if the broker dies

		#setting
		self.zk_object.set(self.leader_node, to_bytes(self.leader)) #setting the port info into the leader znode for pubs + subs
		self.history_node = '/history/node'

		self.threading = threading.Thread(target=self.background_input)
        self.threading.daemon = True
        self.threading.start

    def new_sub(self):
    	print("press x to get history")
    	while True:
    		new_input = raw_input()
    		if new_input == "x" or new_input == "X":
    			@self.zk_object.DataWatch(self.history_node)
                def watch_node(data, stat, event):
                    if event == None: #wait for event to be alive and None(stable)
                        data, stat = self.zk_object.get(self.history_node)
                        print("new sub")
                        address = data.split(",")
                        pub_url = "tcp://127.0.0.1:" + address[1]
                        self.sub_port = address[1]
                        self.newSub = True

    def history(self, hist_list, index, history, message):
    	if len(hist_list[index]) < history:
    		hist_list.append(message)
    	else:
    		hist_list[index].pop(0)
    		hist_list[index].append(message)
    	return hist_list


	def device(self):
		#start the proxy device /broker that forwards messages. same as Assignment 1
		zmq.device(zmq.FORWARDER, self.frontend, self.backend)

	#essentially start  - watches the leader node and re-elects if it disappears
	def monitor(self):
		while True:
			#creating the watch 
			@self.zk_object.DataWatch(self.watch_dir)
			def watch_node(data, stat, event):
				#url's for unbinding before the information is lost
				addr = self.leader.split(",")
				front_url = "tcp://127.0.0.1:" + addr[0]
				back_url = "tcp://127.0.0.1:" + addr[1]
				
				#re-elect if the znode (and thus by proxy - the broker) dies
				if event != None:
					if event.type == "DELETED":
						#same election code as above
						self.election = self.zk_object.Election(self.path, "leader")
						potential_leaders = self.election.contenders()
						self.leader = str(potential_leaders[-1]) 
						#set node with new ports info for pubs + subs
						self.zk_object.set(self.leader_node, self.leader)

						#unbind + re-bind broker ports to new leader's ports
						addr = self.leader.split(",")
						self.frontend.unbind(front_url)
						self.backend.unbind(back_url)
						self.frontend.bind("tcp://127.0.0.1:" + addr[0])
						self.backend.bind("tcp://127.0.0.1:" + addr[1])
			# starts broker
			self.election.run(self.device)

if __name__ == "__main__":
    broker = broker()
    broker.monitor()
