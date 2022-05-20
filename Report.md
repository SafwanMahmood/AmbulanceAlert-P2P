This project was motivated with the idea of providing alerts to cars around ambulance real-time

The main components of the project are:
1) BTPeer : Which sets up the P2P network to process the amubulance and car movement streams.
2) Sensors: Ambulance and cars broadcasting their coordinates to the P2P network using RabbitMq

BTPeer:

This has 3 parts. btpeer.py, btfiler.py, filegui.py

btpeer.py: 
The base class for all the implementation. Has all the functionalities related to sending messages, joining the network, peer handling, main loop functionality.

btfiler.py:
Extends the BTPeer class and implements a wrapper for various handlers to support the use case of pinging peers and all the function of BTPeer class.

The master node is the node that joins the network first, spins up a compute node/peer whenever a new ambulance alert is sent.

Every node after master joins the network and waits for the tasks, and executes the processing after receiving a signal from master.

Each node that is not master has the main loop logic where it connects to RabbitMq after receiving an alert from master and processes the feed from Ambulances and cars and takes necessary actions like alerts. It stores the feed to mysql dump for future analytics.

Has the logic where a master node when shuts down, one of the other nodes is elected as master.


filegui.py:
Its the GUI for the process, we can see the peers in the UI and sets up the object the functionalities classes.


Sensors:

Has two types of sensors, sensorambulance.py, sensorcar.py

Each sends respective feed to the peer node selecting for its computation and processing:

Example for ambulance:

{"id": 0, "timestamp": "2022-05-20 10:51:47.672741", "type": "AMBULANCE", "start": "40.72534335553863,-73.98704614874455", "destination": "40.73324332190101,-73.98298487173007", "current_location": "40.72580732710814,-73.98674320872027", "pulse": 6, "flag": "none"}







