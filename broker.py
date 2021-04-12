
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
		
		self.sub_url = 0
		self.sub_port = 0
		self.newSub = False 
		
		self.poller = zmq.Poller ()
		self.poller.register(self.backend ,zmq.POLLIN)
		self.poller.register(self.frontend, zmq.POLLIN)
		
		self.topic_q = [] #content queue for tickers
		self.topic_index = 0
		self.tickers = [] #list of tickers

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
		self.frontend.bind("tcp://127.0.0.1:" + addr[0])  #will want to modify ip as usual
		self.backend.bind("tcp://127.0.0.1:" + addr[1])

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
		
		self.threading = threading.Thread(target=self.new_sub)
		self.threading.daemon = True
		self.threading.start
		
		
	def new_sub(self):
		print("press x to send history")
		while True:
			new_input = raw_input()
			if new_input == "x" or new_input == "X":
				@self.zk_object.DataWatch(self.history_node)
				def watch_node(data, stat, event):
					if event == None: 
						data, stat = self.zk_object.get(self.history_node)
						print("new sub")
						address = data.split(",")
						pub_addr = "tcp://127.0.0.1:" + address[1]
						self.sub_url = pub_addr
						self.sub_port = address[1]
						self.newSub = True
		
		
	def history(self, hist_list, index, history, message):
		if len(hist_list[index]) < history:
			hist_list.append(message)
		else:
			hist_list[index].pop(0)
			hist_list[index].append(message)
		return hist_list


	def send(self):
		data = dict(self.poller.poll(5000)) # number = time-out in milliseconds
		if self.backend in data:
			string = self.backend.recv()
			topic, messagedata, strength, history = string.split()

		if topic not in self.tickers:
			self.tickers.append(topic)
			strength = 0
			prior_strength = 0
			count = 0
			hist_list = []
			strength_list = []
			topic_index = 0
			message = []
			prior_message = []

			full_data = [strength, prior_strength, count, hist_list, strength_list, topic_index, message, prior_message]
			self.topic_q.append(full_data)
			topic_msg, hist_list, strength, strength_list = self.schedule(self.topic_q[topic_index], string)
			self.topic_index +=1
		else:
			topic_ind = self.tickers.index(topic)
			topic_msg, hist_list, strength, strength_list = self.schedule(self.topic_q[topic_index], string)
			
		if self.newSub: #handling hist for new sub
			ctx = zmq.Context()
			pub = ctx.socket(zmq.PUB)
			pub.bind(self.sub_url)
			if ownership == max(strength_list):
				cur_index = strength_list.index(strength)
				for i in range(len(hist_list)):
					pub.send_multipart (histry_msg[i])
					time.sleep(0.1)
			pub.unbind(self.sub_url)
			pub.close()
			ctx.term()
			general_addr = "tcp://*:" + self.sub_port
			self.frontend.bind(general_addr)
			self.newSub = False
			print("--- Sent HISTORY ---")
		else:
			self.frontend.send_multipart(topic_msg) 
			
		if self.frontend in data: #a subscriber comes here
			string = self.frontend.recv()
			self.backend.send_multipart(string)

	def schedule(self, info, string):
		[strength, prior_strength, count, hist_list, strength_list, topic_index, message, prior_message] = info
		num = 20
		content = string
		topic, messagedata, new_strength, history = string.split()

		if strength not in strength_list:
			strength_list.append(strength)
			hist_list.append([])
			hist_list = self.hist_list(hist_list, topic_index, history, string)
			topic_index += 1 # the actual size of the publishers
		else:
			topic_ind = strength_list.index(strength)
			hist_list = self.hist_list(hist_list, topic_ind, history, string)
			
		if new_strength > strength:
			prior_strength = strength
			strength = new_strength
			prior_message = message
			message = string
			count = 0
		elif new_strength == strength:
			message = string
			count = 0
		else:
			count +=1
			
		if count >= num:
			strength = prior_strength
			message = prior_message
			count = 0
		info[0] = strength
		info[1] = prior_strength
		info[2] = count
		info[3] = hist_list
		info[4] = strength_list
		info[5] = topic_index
		info[6] = prior_message
		info[7] = message
		
		hist_index = strength_list.index(strength)
		hist_msg = hist_list[hist_index]
		return message, hist_msg, new_strength, strength_list

	#watch self z-node and re-elect + restart if needed
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
			self.election.run(self.send)

if __name__ == "__main__":
	broker = broker()
	broker.monitor()
