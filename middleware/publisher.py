import sys
import zmq
from threading import Thread
import random
import datetime
import os
import time
from queue import Queue

class publisher(Thread):

	def __init__(self, id, topic, flood):
		super().__init__()
		self.id = int(id)
		self.topic = topic
		self.flood = flood
		self.joined = True

		# connect to the lead broker and start zk watch.
		self.broker = broker_add
		self.path = '/leader/node'
		# port from zk config file, host ip should be changed
		self.zk_object = KazooClient(hosts='127.0.0.1:2181')
		self.zk_object.start()

		# flooding approach
		self.context = zmq.Context()
		self.pub = self.context.socket(zmq.PUB)
		if self.flood == True:
			self.pub.bind("tcp://127.0.0.1:" + str(5558 + self.id))
		# broker approach
		else:
			data, stat = self.zk_object.get(self.path)
			data = str(data)  # casting from bytes -> string
			addr = data.split(",")
			addr[1] = addr[1][:-1]  # getting the xpub port and removing the " b' " from casting from byte to string
			print("tcp://" + self.broker + ":" + addr[1])
			self.pub.connect("tcp://" + self.broker + ":" + addr[1])

	def run(self):
		print('starting publisher number ' + str(self.id))
		context = zmq.Context()
		pub = context.socket(zmq.PUB)
		print('Flood variable is ' + self.flood)
		if self.flood != True:
			print('Flooding Approach Enabled for Publisher')

			port = int(5558 + self.id)

			#FIXME: Edit the binding to seek the appropriate IP 10.0.0.#
			#pub.bind("tcp://10.0.0." + str(self.id) + ":" + str(5558 + self.id))
			print("tcp://10.0.0.{ipid}:{portid}".format(ipid=self.id, portid=port))
			#pub.bind("tcp://10.0.0." + int(self.id) + ':' + port)

			pub.bind("tcp://10.0.0.{ipid}:{portid}".format(ipid=self.id, portid=port))
			#Working pub.bind("tcp://10.0.0.{ipid}:{portid}".format(ipid=self.id, portid=port))
			#pub.bind("tcp://10.0.0.{ipid}:{portid}".format(ipid=self.id, portid=(port)))

		else:
			print('Broker Approach Enabled for Publisher')
			pub.connect("tcp://10.0.0.1:5560")

		while self.joined:

			@self.zk_object.DataWatch(self.path)
			def watch_node(data, stat, event):
				# required check so we can look at type
				if event != None:
					# if broker fails - restart self + reconnect
					if event.type == "CHANGED":
						self.pub.close()
						self.context.term()
						time.sleep(2)
						self.context = zmq.Context()
						self.pub = self.context.socket(zmq.PUB)
						# same recasting + connection as above
						data, stat = self.zk_object.get(self.path)
						data = str(data)
						addr = data.split(",")
						addr[1] = addr[1][:-1]
						self.pub.connect("tcp://" + self.broker + ":" + addr[1])

			#select a stock
			stock_list = ["GOOG", "AAPL", "MSFT", "IBM", "AMD", "CLII", "EXO", "NFLX", "CME", "CKA"]
			index = random.randrange(1,10)
			#topic = stock_list[index]
			#generate a random price
			price = str(random.randrange(20, 60))
			#send topic + price to broker

			# TODO: Push Stock and Price to Queue (With Max_Size)
			# TODO: Whether to tag time at creation of a tick or at the sending of the ticks
			# Keep vestigial time of publication, and then append time of sending the tick
			# TODO: Protocol for Offered vs. Requested Dominance Relationship

			#Capturing system time
			#seconds = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
			#time = seconds
			time_started = (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()

			for tick in tick_list:
				buffer.enqueue("{topic} {price} {time_started}".format(topic=self.topic, price=price, time_started=time_started))

			pub.send_string("{topic} {price} {time_started}".format(topic=self.topic, price=price, time_started=time_started))
			time.sleep(1)

	def leave(self):
		self.joined = False
		print('pub leaving')

	# TODO: Create MAX_SIZE of buffer using LIFO Queue for the subscriber to pull from the publisher
	def buffer(self):
		self.queue = Queue()


def main():
	id = sys.argv[1]
	topic = sys.argv[2]
	method = sys.argv[3]

	pub = publisher(id, topic, method)
	pub.start()
	#while True:
		#pub.start()
		#time.sleep(1)

if __name__=='__main__':
	main()