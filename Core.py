import sys
import time

from MyOwnPeer2PeerNode import MyOwnPeer2PeerNode

#add node to data base first, then pull the id, and append that
#to the end of the MyOwnPeer2PeerNode call
node = MyOwnPeer2PeerNode("127.0.0.1", 10001)
time.sleep(1)

# Do not forget to start your node!
node.start()
time.sleep(1)

running = True
isMaster = False
checkIfMasterAvailable = databaseCall()



#If checkIfMasterAvailable is True, connect with master node
if checkIfMasterAvailable
    node.connect_with_node('127.0.0.1', 10002)
    isMaster = False
    time.sleep(2)
else
    isMaster = True
    time.sleep(2)

# Example of sending a message to the nodes (dict).
#node.send_to_nodes({"message": "Hi there!"})

time.sleep(5) # Create here your main loop of the application
while running:
    if isMaster:
        
    else:
        

node.stop()


def delegate:
    return None
    
def accessNodeList:
    return None
    
def accessActivityList:
    return None

def setDataAccess:
    #? unsure of this one's use
    return None
    
def shutDown(isMaster):
    if isMaster:
        #master node shutdown
    else:
        #comp node shutdown
    return None

def getData:
    return None
    
def sendToAlertStream(data):
    return None
    
def computeDistance(data):
    return None
    
def addVehicleToSystem(vehicle):
    return None
    
def addAmbulanceToSystem(ambulance):
    return None
    
def accessInputStream:
    return None
