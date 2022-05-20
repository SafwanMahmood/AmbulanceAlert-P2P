#!/usr/bin/python

# btpeer.py

import socket
import struct
import threading
import time
import traceback
import pika
import json
import mysql.connector


def btdebug( msg ):
    """ Prints a messsage to the screen with the name of the current thread """
    print("[%s] %s" % ( str(threading.currentThread().getName()), msg ))


#==============================================================================
class BTPeer(object):
    """ Implements the core functionality that might be used by a peer in a
    P2P network.

    """

    #--------------------------------------------------------------------------
    def __init__( self, maxpeers, serverport, myid=None, serverhost=None ):
    #--------------------------------------------------------------------------
        """ Initializes a peer servent (sic.) with the ability to catalog
        information for up to maxpeers number of peers (maxpeers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (serverhost) will be determined by attempting to connect to an
        Internet host like Google.

        """
        self.debug = 1

        self.maxpeers = int(maxpeers)
        self.serverport = int(serverport)
        self.isMaster = False
        self.flag = False
        self.needsToProcess = False
        if serverhost: self.serverhost = serverhost
        else: self.__initserverhost()

        if myid: self.myid = myid
        else: self.myid = '%s:%d' % (self.serverhost, self.serverport)

        self.peerlock = threading.Lock()  # ensure proper access to
                                    # peers list (maybe better to use
                                    # threading.RLock (reentrant))
        self.peers = {}        # peerid ==> (host, port) mapping
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

        try:
            fp = open('masterport.txt', 'r')
            line = fp.readline()
            masterip, masterport = line.split(":")
            masterport = int(masterport)
            if serverport == masterport:
                self.isMaster = True
        finally:
            fp.close()

    #--------------------------------------------------------------------------
    def __initserverhost( self ):
    #--------------------------------------------------------------------------
        """ Attempt to connect to an Internet host in order to determine the
        local machine's IP address.

        """
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.connect( ( "www.google.com", 80 ) )
        self.serverhost = s.getsockname()[0]
        s.close()



    #--------------------------------------------------------------------------
    def __debug( self, msg ):
    #--------------------------------------------------------------------------
        if self.debug:
            btdebug( msg )



    #--------------------------------------------------------------------------
    def __handlepeer( self, clientsock ):
    #--------------------------------------------------------------------------
        """
        handlepeer( new socket connection ) -> ()

        Dispatches messages from the socket connection
        """

        self.__debug( 'New child ' + str(threading.currentThread().getName()) )
        self.__debug( 'Connected ' + str(clientsock.getpeername()) )

        host, port = clientsock.getpeername()
        peerconn = BTPeerConnection( None, host, port, clientsock, debug=False )
        
        try:
            msgtype, msgdata = peerconn.recvdata()
            if msgtype: msgtype = msgtype.upper()
            if msgtype not in self.handlers:
                self.__debug( 'Not handled: %s: %s' % (msgtype, msgdata) )
            else:
                self.__debug( 'Handling peer msg: %s: %s' % (msgtype, msgdata) )
                self.handlers[ msgtype ]( peerconn, msgdata )
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
        
        self.__debug( 'Disconnecting ' + str(clientsock.getpeername()) )
        peerconn.close()

    # end handlepeer method



    #--------------------------------------------------------------------------
    def __runstabilizer( self, stabilizer, delay ):
    #--------------------------------------------------------------------------
        while not self.shutdown:
            stabilizer()
            time.sleep( delay )

	    

    #--------------------------------------------------------------------------
    def setmyid( self, myid ):
    #--------------------------------------------------------------------------
        self.myid = myid



    #--------------------------------------------------------------------------
    def startstabilizer( self, stabilizer, delay ):
    #--------------------------------------------------------------------------
        """ Registers and starts a stabilizer function with this peer. 
        The function will be activated every <delay> seconds. 

        """
        t = threading.Thread( target = self.__runstabilizer, 
                    args = [ stabilizer, delay ] )
        t.start()

	

    #--------------------------------------------------------------------------
    def addhandler( self, msgtype, handler ):
    #--------------------------------------------------------------------------
        """ Registers the handler for the given message type with this peer """
        assert len(msgtype) == 4
        self.handlers[ msgtype ] = handler



    #--------------------------------------------------------------------------
    def addrouter( self, router ):
    #--------------------------------------------------------------------------
        """ Registers a routing function with this peer. The setup of routing
        is as follows: This peer maintains a list of other known peers
        (in self.peers). The routing function should take the name of
        a peer (which may not necessarily be present in self.peers)
        and decide which of the known peers a message should be routed
        to next in order to (hopefully) reach the desired peer. The router
        function should return a tuple of three values: (next-peer-id, host,
        port). If the message cannot be routed, the next-peer-id should be
        None.

        """
        self.router = router



    #--------------------------------------------------------------------------
    def addpeer( self, peerid, host, port ):
    #--------------------------------------------------------------------------
        """ Adds a peer name and host:port mapping to the known list of peers.
        
        """
        if peerid not in self.peers and (self.maxpeers == 0 or
                        len(self.peers) < self.maxpeers):
            self.peers[ peerid ] = (host, int(port))
            return True
        else:
            return False



    #--------------------------------------------------------------------------
    def getpeer( self, peerid ):
    #--------------------------------------------------------------------------
        """ Returns the (host, port) tuple for the given peer name """
        assert peerid in self.peers    # maybe make this just a return NULL?
        return self.peers[ peerid ]



    #--------------------------------------------------------------------------
    def removepeer( self, peerid ):
    #--------------------------------------------------------------------------
        """ Removes peer information from the known list of peers. """
        if peerid in self.peers:
            del self.peers[ peerid ]



    #--------------------------------------------------------------------------
    def addpeerat( self, loc, peerid, host, port ):
    #--------------------------------------------------------------------------
        """ Inserts a peer's information at a specific position in the 
        list of peers. The functions addpeerat, getpeerat, and removepeerat
        should not be used concurrently with addpeer, getpeer, and/or 
        removepeer. 

        """
        self.peers[ loc ] = (peerid, host, int(port))



    #--------------------------------------------------------------------------
    def getpeerat( self, loc ):
    #--------------------------------------------------------------------------
        if loc not in self.peers:
            return None
        return self.peers[ loc ]



    #--------------------------------------------------------------------------
    def removepeerat( self, loc ):
    #--------------------------------------------------------------------------
        removepeer( self, loc ) 



    #--------------------------------------------------------------------------
    def getpeerids( self ):
    #--------------------------------------------------------------------------
        """ Return a list of all known peer id's. """
        return self.peers.keys()



    #--------------------------------------------------------------------------
    def numberofpeers( self ):
    #--------------------------------------------------------------------------
        """ Return the number of known peer's. """
        return len(self.peers)


    
    #--------------------------------------------------------------------------
    def maxpeersreached( self ):
    #--------------------------------------------------------------------------
        """ Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if maxpeers is set to
        0.

        """
        assert self.maxpeers == 0 or len(self.peers) <= self.maxpeers
        return self.maxpeers > 0 and len(self.peers) == self.maxpeers



    #--------------------------------------------------------------------------
    def makeserversocket( self, port, backlog=5 ):
    #--------------------------------------------------------------------------
        """ Constructs and prepares a server socket listening on the given 
        port.

        """
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
        s.bind( ( '', port ) )
        s.listen( backlog )
        return s



    #--------------------------------------------------------------------------
    def sendtopeer( self, peerid, msgtype, msgdata, waitreply=True ):
    #--------------------------------------------------------------------------
        """
        sendtopeer( peer id, message type, message data, wait for a reply )
        -> [ ( reply type, reply data ), ... ] 

        Send a message to the identified peer. In order to decide how to
        send the message, the router handler for this peer will be called.
        If no router function has been registered, it will not work. The
        router function should provide the next immediate peer to whom the 
        message should be forwarded. The peer's reply, if it is expected, 
        will be returned.

        Returns None if the message could not be routed.
        """

        if self.router:
            nextpid, host, port = self.router( peerid )
        if not self.router or not nextpid:
            self.__debug( 'Unable to route %s to %s' % (msgtype, peerid) )
            return None
        #host,port = self.peers[nextpid]
        return self.connectandsend( host, port, msgtype, msgdata,
                        pid=nextpid,
                        waitreply=waitreply )
    


    #--------------------------------------------------------------------------
    def connectandsend( self, host, port, msgtype, msgdata, 
			pid=None, waitreply=True ):
    #--------------------------------------------------------------------------
        """
        connectandsend( host, port, message type, message data, peer id,
        wait for a reply ) -> [ ( reply type, reply data ), ... ]

        Connects and sends a message to the specified host:port. The host's
        reply, if expected, will be returned as a list of tuples.

        """
        msgreply = []
        try:
            peerconn = BTPeerConnection( pid, host, port, debug=self.debug )
            peerconn.senddata( msgtype, msgdata )
            self.__debug( 'Sent %s: %s' % (pid, msgtype) )
            
            if waitreply:
                onereply = peerconn.recvdata()
                while (onereply != (None,None)):
                    msgreply.append( onereply )
                    self.__debug( 'Got reply %s: %s' 
                        % ( pid, str(msgreply) ) )
                    onereply = peerconn.recvdata()
                peerconn.close()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
        
        return msgreply

    # end connectsend method



    #--------------------------------------------------------------------------
    def checklivepeers( self ):
    #--------------------------------------------------------------------------
        """ Attempts to ping all currently known peers in order to ensure that
        they are still active. Removes any from the peer list that do
        not reply. This function can be used as a simple stabilizer.

        """
        todelete = []
        for pid in self.peers:
            isconnected = False
            try:
                self.__debug( 'Check live %s' % pid )
                host,port = self.peers[pid]
                peerconn = BTPeerConnection( pid, host, port, debug=self.debug )
                peerconn.senddata( 'PING', '' )
                isconnected = True
            except:
                todelete.append( pid )
                if isconnected:
                    peerconn.close()

        self.peerlock.acquire()
        try:
            for pid in todelete: 
                if pid in self.peers: del self.peers[pid]
        finally:
            self.peerlock.release()
        # end checklivepeers method



    #--------------------------------------------------------------------------
    def mainloop( self ):
    #--------------------------------------------------------------------------
        s = self.makeserversocket( self.serverport )
        s.settimeout(None)
        self.__debug( 'Server started: %s (%s:%d)'
                % ( self.myid, self.serverhost, self.serverport ) )
        pulseIndex = 1
        self.hasSentData = False
        self.hasNotSent = True
        ambulanceSET = set()
        nodeIndexCount = 0
    
        while not self.shutdown:
            try:
                self.__debug( 'Listening for connections...' )
                clientsock, clientaddr = s.accept()
                clientsock.settimeout(10)

                t = threading.Thread( target = self.__handlepeer,
                            args = [ clientsock ] )
                t.start()
                #print "seperate code"
                #print self.flag
                if self.isMaster and not self.hasSentData:
                    #change localhost to ip
                    print("is Master Node")
                    if len(self.getpeerids()) >= 1:
                        print("Found peers")
                        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', blocked_connection_timeout=2))
                        channel = connection.channel()

                        channel.exchange_declare(exchange='logs', exchange_type='fanout')

                        result = channel.queue_declare(queue='', exclusive=True)
                        queue_name = result.method.queue

                        channel.queue_bind(exchange='logs', queue=queue_name)

                        count = 0

                        # Get ten messages and break out
                        for method_frame, properties, body in channel.consume(queue=queue_name):

                            # Display the message parts
                            # print(method_frame)
                            # print(properties)
                            print(body)
                            print("sending to compute node")
                            if body == "START":
                                keys = self.getpeerids()
                                neededid = None
                                for k in keys:
                                    #print (k, self.peers.get(k))
                                    temphost, tempport = k.split(":")
                                    if (temphost, tempport) != (self.serverhost, self.serverport) and (k not in ambulanceSET):
                                        neededid = k

                                    #SUPER BAD MULTHREADING PROBLEM
                                    #if keys.index(k) == nodeIndexCount:
                                        #nodeIndexCount = nodeIndexCount + 1
                                        #print (temphost,tempport)
                                #print neededid
                                computehost, computeport = neededid.split(":")
                                ambulanceSET.add(neededid)

                                #print computehost
                                #print computeport
                                self.sendtopeer(neededid, "INIT", str(computehost)+","+str(computeport)+","+"INIT")
                                print("sent "+str(computehost)+","+str(computeport)+","+"INIT")

                            count = count + 1

                            # Acknowledge the message
                            channel.basic_ack(method_frame.delivery_tag)

                            print("I sent msg")

                            # Escape out of the loop after 10 messages
                            if count == 1:
                                count = 0
                                self.hasSentData = True
                                break

                        # Cancel the consumer and return any pending messages
                        requeued_messages = channel.cancel()

                        # Close the channel and the connection
                        channel.close()
                        connection.close()
                    
                elif self.hasSentData:
                    if self.hasNotSent:
                        self.hasNotSent = False
                        print("data has been sent")
                        '''keys = self.getpeerids()
                        neededid = None
                        for k in keys:
                            if self.peers.get(k) != (serverhost, serverport):
                                neededid = k
                            print(k)
                            print(self.peers.get(k))
                        #It's not sending a message to the compute node
                        peerconn = BTPeerConnection( neededid, "192.168.1.3", 9002 )
                        peerconn.senddata( SENDSTRING, 'Process' )
                        peerconn.close()'''
                elif not self.isMaster:
                    #Compute Node
                    if self.flag:
                        print("Not isMaster")
                        print("Processing")
                        # outerPulseList = []
                        # for i in range(110):
                        #     outerPulseList.append([])
                        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', blocked_connection_timeout=1))
                        channel = connection.channel()

                        channel.exchange_declare(exchange='logs', exchange_type='fanout')

                        result = channel.queue_declare(queue='', exclusive=True)
                        queue_name = result.method.queue

                        channel.queue_bind(exchange='logs', queue=queue_name)

                        # Get ten messages and break out
                        for method_frame, properties, body in channel.consume(queue=queue_name):

                            # Display the message parts
                            # print(method_frame)
                            # print(properties)
                            print(body)
                            #Modify RabbitMQ to either have a single super queue, or multiple queues
                            message = json.loads(body)
                            endFlag = message['flag']
                            id = message['id']
                            print(endFlag)
                            currPulse = message['pulse']
                            
                            # Acknowledge the message
                            
                            #process data and add to list of lists
                            channel.basic_ack(method_frame.delivery_tag)
                            # outerPulseList[currPulse].append(message)
                            
                            #check if data at list of pulse index is full
                            # if len(outerPulseList[pulseIndex]) == 3:
                            #     self.process(outerPulseList[pulseIndex])
                            
                            self.store(body)
                            
                            # EDIT CONDITION TO CHECK IF flag == False
                            if endFlag == "false":
                                print("End batch")
                                # if id in ambulanceSET:
                                #     ambulanceSET.remove(id)
                                # self.flag = False  
                                # channel.close()
                                # connection.close()
                    else:
                        print("Waiting")                  
                        # Cancel the consumer and return any pending messages

                        # Close the channel and the connection
                        
            except KeyboardInterrupt:
                print('KeyboardInterrupt: stopping mainloop')
                self.shutdown = True
                continue
            except:
                if self.debug:
                    traceback.print_exc()
                    continue

        # end while loop
        self.__debug( 'Main loop exiting' )

        # if self.isMaster:
        #     keys = self.getpeerids()
        #     neededid = None
        #     for k in keys:
        #         #print (k, self.peers.get(k))
        #         temphost, tempport = k.split(":")
        #         if (temphost, tempport) != (self.serverhost, self.serverport):
        #             neededid = k
        #         #SUPER BAD MULTHREADING PROBLEM
        #         #if keys.index(k) == nodeIndexCount:
        #             #nodeIndexCount = nodeIndexCount + 1
        #             #print (temphost,tempport)
        #     #print neededid
        #     if neededid is not None:
        #         computehost, computeport = neededid.split(":")
        #         #print computehost
        #         #print computeport
        #         self.sendtopeer(neededid, "SHUT", str(computehost)+","+str(computeport)+","+"SHUT")
        #         print("sent "+str(computehost)+","+str(computeport)+","+"SHUT")
        #     print("shuting down")
            
        #     # s.close()

        # end mainloop method
        
    def setProcessOn( self ):
        self.needsToProcess = True

    def process( self, lst ):
        for i in lst:
            print(i)
        
    def setflag( self, f ):
        self.flag = f

    def setMaster( self, m ):
        self.isMaster = m
        
    def store( self, s ):
        mydb = mysql.connector.connect(
        host="cc-project1-sql.mysql.database.azure.com",
        user="ccprojectadmin@cc-project1-sql",
        password="Admin@123"
        )
        print(mydb)
        message = json.loads(s)
        id = message['id']
        ts = message['timestamp']
        pulse = message['pulse']
        flag = message['flag']
        curr_loc = message['current_location']
        type = message['type']
        cursor = mydb.cursor()
        query = "INSERT INTO archemy.mldump (id, ts, pulse, flag, curr_loc, type) VALUES (%s, %s, %s, %s, %s, %s)"
        values = [(id, ts, pulse, flag, curr_loc, type)]
        cursor.executemany(query, values)
        mydb.commit()
        
        print(cursor.rowcount, "record inserted")
# end BTPeer class

        '''
        {"timestamp": "2022-05-19 19:37:34.919000", "pulse": 60, "flag": "none", "current_location": "40.72998307123379,-73.98347145645799", "type": "CAR", "id": 1}
        '''

# **********************************************************




class BTPeerConnection:

    #--------------------------------------------------------------------------
    def __init__( self, peerid, host, port, sock=None, debug=False ):
    #--------------------------------------------------------------------------
	# any exceptions thrown upwards

        self.id = peerid
        self.debug = debug

        if not sock:
            self.s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            self.s.connect( ( host, int(port) ) )
        else:
            self.s = sock

        self.sd = self.s.makefile( 'rw', 0 )


    #--------------------------------------------------------------------------
    def __makemsg( self, msgtype, msgdata ):
    #--------------------------------------------------------------------------
        msglen = len(msgdata)
        msg = struct.pack( "!4sL%ds" % msglen, msgtype, msglen, msgdata )
        return msg


    #--------------------------------------------------------------------------
    def __debug( self, msg ):
    #--------------------------------------------------------------------------
        if self.debug:
            btdebug( msg )


    #--------------------------------------------------------------------------
    def senddata( self, msgtype, msgdata ):
    #--------------------------------------------------------------------------
        """
        senddata( message type, message data ) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """

        try:
            msg = self.__makemsg( msgtype, msgdata )
            self.sd.write( msg )
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return False
        return True
	    

    #--------------------------------------------------------------------------
    def recvdata( self ):
    #--------------------------------------------------------------------------
        """
        recvdata() -> (msgtype, msgdata)

        Receive a message from a peer connection. Returns (None, None)
        if there was any error.
        """

        try:
            msgtype = self.sd.read( 4 )
            if not msgtype: return (None, None)
            
            lenstr = self.sd.read( 4 )
            msglen = int(struct.unpack( "!L", lenstr )[0])
            msg = ""

            while len(msg) != msglen:
                data = self.sd.read( min(2048, msglen - len(msg)) )
                if not len(data):
                    break
                msg += data

                if len(msg) != msglen:
                    return (None, None)

        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
                return (None, None)

        return ( msgtype, msg )

    # end recvdata method


    #--------------------------------------------------------------------------
    def close( self ):
    #--------------------------------------------------------------------------
        """
        close()

        Close the peer connection. The send and recv methods will not work
        after this call.
        """

        self.s.close()
        self.s = None
        self.sd = None


    #--------------------------------------------------------------------------
    def __str__( self ):
    #--------------------------------------------------------------------------
	    return "|%s|" % peerid




