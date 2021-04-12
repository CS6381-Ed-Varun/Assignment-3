# CS6381 Assignment 3

## Overview
In this assignment we will build upon the middleware from Assignment 2. In order to create a history for new subscribers and ownership strength, we have used Zookeeper to impliment a history and create a more complex broker capable of pairing based on ownership strength and history. 

| Simple Broker Latency                                                                                                    | Complex Broker Latency                                                                                                     | Simple Flooding Latency                                                                                                      | Complex Flooding Latency                                                                                                       |
|--------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| ![simple_broker](https://github.com/edmasters/single_broker_pub_sub/blob/automated-local-host/results/simple_broker.png) | ![complex_broker](https://github.com/edmasters/single_broker_pub_sub/blob/automated-local-host/results/complex_broker.png) | ![simple flooding](https://github.com/edmasters/single_broker_pub_sub/blob/automated-local-host/results/simple_flooding.png) | ![complex flooding](https://github.com/edmasters/single_broker_pub_sub/blob/automated-local-host/results/complex_flooding.png) |

## Manual Installation

### Dependencies
- Java: sudo apt-get install openjdk-9-jre-headless
- libtools: sudo apt-get install libtool
- Zookeeper: http://www.gtlib.gatech.edu/pub/apache/zookeeper/ <br/>
      -Uncompress: tar xvzf zookeeper-'version'.tar.gz
- Kazoo: pip install python3 Kazoo


## Before running the scripts

- Start the ZooKeeper server
```
sudo bin/zkServer.sh start
```


## Mininet Emulation
Assign the various hosts as either publisher or subscriber. 
Subsequently, passing the parameters for the stock ticker and API method will generate the connections.
The latencies will be exported to a csv and an analysis can be replicated with offline_analysis.py

### Simple Broker Approach
```bash
sudo mn -c #For cleaning up the environment
sudo mn --topo single,4 -x
mininet> h1 python3 ./middleware/broker.py &
mininet> h2 python3 ./middleware/subscriber.py AAPL True &
mininet> h3 python3 ./middleware/listener.py True &
mininet> h4 python3 ./middleware/publisher.py 1 AAPL True &
```

### Complex Broker Approach
```bash
sudo mn -c #For cleaning up the environment
sudo mn --topo single,8 -x
mininet> h1 python3 ./middleware/broker.py &
mininet> h2 python3 ./middleware/subscriber.py AAPL True &
mininet> h3 python3 ./middleware/subscriber.py MSFT True &
mininet> h4 python3 ./middleware/subscriber.py NFLX True &
mininet> h5 python3 ./middleware/publisher.py 5 AAPL True &
mininet> h6 python3 ./middleware/publisher.py 6 MSFT True &
mininet> h7 python3 ./middleware/publisher.py 7 NFLX True &
mininet> h8 python3 ./middleware/listener.py True &
```

### Simple Flood Approach
```bash
sudo mn -c #For cleaning up the environment
sudo mn --topo single,4 -x
mininet> h1
mininet> h2 python3 ./middleware/subscriber.py AAPL False &
mininet> h3 python3 ./middleware/listener.py False &
mininet> h4 python3 ./middleware/publisher.py 4 AAPL False &
```

### Complex Flood Approach
```bash
sudo mn -c #For cleaning up the environment
sudo mn --topo single,8 -x
mininet> h1 python3 ./middleware/subscriber.py AAPL False &
mininet> h2 python3 ./middleware/subscriber.py MSFT False &
mininet> h3 python3 ./middleware/subscriber.py NFLX False &
mininet> h4 python3 ./middleware/publisher.py 4 AAPL False &
mininet> h5 python3 ./middleware/publisher.py 5 MSFT False &
mininet> h6 python3 ./middleware/publisher.py 6 NFLX False &
mininet> h7 python3 ./middleware/listener.py False &
```
