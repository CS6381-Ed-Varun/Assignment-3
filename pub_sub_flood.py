import sys
import zmq
from threading import Thread
import random
import time
import datetime
from kazoo.client import KazooClient
from kazoo.client import KazooState
import logging

#logging required by zookeeper -ignore
logging.basicConfig()

class subscriber(Thread):

	def __init__(self, topic, flood, broker_add):
		super().__init__()
		self.topic = topic
		self.flood = flood
		self.joined = True
		
		#connect to the socket like normal. self.sub = our sub socket
		self.context = zmq.Context()
		self.sub = self.context.socket(zmq.SUB)
		self.sub.setsockopt_string(zmq.SUBSCRIBE, self.topic)

		#initializing zookeeper
		self.path = '/leader/node'
		#setting the broker ip which is 'known' so we can use it as an input
		self.broker = broker_add
		#where zk is hosted - change local host part as needed and port 2181 comes from the config file
		self.zk_object = KazooClient(hosts='127.0.0.1:2181')
		self.zk_object.start()
		
		#flooding connection
		if self.flood == True:
			for i in range(1,6):
				port = str(5560 + i)
				self.sub.connect("tcp://127.0.0.1:" + port)
			print("flooding sub")
		#connecting tp the broker using zookeeper node on leader broker
		else:
			data, stat = self.zk_object.get(self.path) #get port #'s from the leader's zk node
			data = str(data) 
			addr = data.split(",")  #type casting since path is bytes and strings are needed for connect
			addr[0] = addr[0][2:]   #removing a ' from the byte -> string cast
			print("tcp://" + self.broker + ":" + addr[0])	
			self.sub.connect("tcp://" + self.broker + ":" + addr[0]) #connecting to the broker

	def run(self):
		print('starting sub ' + self.topic)
		while self.joined:
			#setting our zk watch on the broker node
			@self.zk_object.DataWatch(self.path)
			def watch_node(data, stat, event): 
				#required to allow us to check the type lower down
				if event != None:	
					#if anything happens to the znode: close the connections, sleep, and then reconnect using new leader ports
					if event.type == "CHANGED":
						self.sub.close()
						self.context.term()
						time.sleep(2)
						self.context = zmq.Context()
						self.sub = self.context.socket(zmq.SUB)
						#reconnect to broker
						data, stat = self.zk_object.get(self.path)
						data = str(data)
						addr = data.split(",") #same type casting as above for bytes -> string
						addr[0] = addr[0][2:]
						self.sub.connect("tcp://" + self.broker + ":" + addr[0])

			string = self.sub.recv()
			topic, messagedata, time_started = string.split()
			print (topic, messagedata)
			time_received = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
			latency = format((1000 * (float(time_received) - float(time_started))), '.5f')
			print(topic, messagedata, latency, 'ms')
			with open("./results/latency_{}.csv".format(topic), "a") as f:
				f.write(str(latency) + "\n")
			
	#for leaving 
	def close(self):
		self.joined = False
		print('sub ' + self.topic + ' leaving')

class publisher(Thread):

	def __init__(self, id, flood, topic, broker_add):
		super().__init__()
		self.id = id
		self.flood = flood
		self.joined = True
		self.topic = topic

		#connect to the lead broker and start zk watch. 
		self.broker = broker_add
		self.path = '/leader/node'
		#port from zk config file, host ip should be changed
		self.zk_object = KazooClient(hosts='127.0.0.1:2181')
		self.zk_object.start()
		
		#flooding approach
		self.context = zmq.Context()
		self.pub = self.context.socket(zmq.PUB)
		if self.flood == True:
			self.pub.bind("tcp://127.0.0.1:" + str(5560 + self.id))
			print("flooding pub")
		#broker approach 
		else:
			data, stat = self.zk_object.get(self.path)
			data = str(data) #casting from bytes -> string 
			addr = data.split(",") 
			addr[1] = addr[1][:-1] #getting the xpub port and removing the " b' " from casting from byte to string
			print("tcp://" + self.broker + ":" + addr[1])
			self.pub.connect("tcp://" + self.broker + ":" + addr[1])



	def run(self):
		print('starting publisher ' + self.topic)
		#select a stock
		while self.joined:

			#set-up the znode watch to see if a broker goes down
			@self.zk_object.DataWatch(self.path)
			def watch_node(data, stat, event):
				#required check so we can look at type 
				if event != None:	
				#if broker fails - restart self + reconnect
					if event.type == "CHANGED":
						self.pub.close()
						self.context.term()
						time.sleep(2)
						self.context = zmq.Context()
						self.pub = self.context.socket(zmq.PUB)
						#same recasting + connection as above
						data, stat = self.zk_object.get(self.path)
						data = str(data)
						addr = data.split(",")
						addr[1] = addr[1][:-1]
						self.pub.connect("tcp://" + self.broker + ":" + addr[1])

			#generate a random price
			price = str(random.randrange(20, 60))
			#send ticker + price to broker
			#self.pub.send_string("%s %s" % (self.topic, price))
			#time.sleep(1)

			time_started = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
			self.pub.send_string("{topic} {price} {time_started}".format(topic=self.topic, price=price, time_started=time_started))
			time.sleep(1)


	def close(self):
		self.joined = False
		print('pub leaving')

class listener(Thread):
	
	#init self
	def __init__(self, flood):
		super().__init__()
		self.flood = flood
		self.joined = True
		self.broker = broker_add
		self.path = '/leader/node'
		#port from zk config file, host ip should be changed
		self.zk_object = KazooClient(hosts='127.0.0.1:2181')
		self.zk_object.start()

	#start up the thread
	def run(self):
		print("starting listener thread")
		context = zmq.Context()
		sub = context.socket(zmq.SUB)
		#Flooding version - connect to all pub networks w/o a filter
		if self.flood == True: 
			for i in range(1,8):
				port = str(5560 + i)
				sub.connect("tcp://127.0.0.1:" + port)
				sub.setsockopt_string(zmq.SUBSCRIBE, "")
		#Broker version - connect w/o filtering
		else:
			data, stat = self.zk_object.get(self.path) #get port #'s from the leader's zk node
			data = str(data) 
			addr = data.split(",")  #type casting since path is bytes and strings are needed for connect
			addr[0] = addr[0][2:]   #removing a ' from the byte -> string cast
			print("tcp://" + self.broker + ":" + addr[0])	
			self.sub.connect("tcp://" + self.broker + ":" + addr[0]) #connecting to the broker
			sub.setsockopt_string(zmq.SUBSCRIBE, "")
		
		#make a list of messages and appended to it
		messages = []
		while self.joined:
			#setting our zk watch on the broker node
			@self.zk_object.DataWatch(self.path)
			def watch_node(data, stat, event): 
				#required to allow us to check the type lower down
				if event != None:	
					#if anything happens to the znode: close the connections, sleep, and then reconnect using new leader ports
					if event.type == "CHANGED":
						self.sub.close()
						self.context.term()
						time.sleep(2)
						self.context = zmq.Context()
						self.sub = self.context.socket(zmq.SUB)
						#reconnect to broker
						data, stat = self.zk_object.get(self.path)
						data = str(data)
						addr = data.split(",") #same type casting as above for bytes -> string
						addr[0] = addr[0][2:]
						self.sub.connect("tcp://" + self.broker + ":" + addr[0])
						
			string = sub.recv()
			messages.append(messages)
			if (len(messages) % 10 == 0):
				print(str(len(messages)) + " messages sent by brokers")
	def close(self):
		self.joined = False
		print('listener leaving')
		
			
#initializing the individual pubs, sub, and listener
def main():
        
	s1 = subscriber('MSFT', True, '127.0.0.1')
	s1.start()

	#s2 = subscriber('AAPL', True, '127.0.0.1')
	#s2.start()

	#s3 = subscriber('IBM', True, '127.0.0.1')
	#s3.start()
	
	#we may want to pre-set the stock in a refactor since we'll need a known topic for Assignment 3
	p1 = publisher(1, True, 'MSFT', '127.0.0.1')
	p1.start()

	#p2 = publisher(2, True, 'AAPL', '127.0.0.1')
	#p2.start()

	#p3 = publisher(3, True, 'IBM', '127.0.0.1')
	#p3.start()

	#s1.close()

if __name__ == "__main__":
    main()

