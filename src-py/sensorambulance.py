
import pandas as pd
import numpy as np
import pika
import sys
import time
import json


import socket
  



df = pd.read_csv('ambu-sim1.csv',header=None)

lat = np.array(df[0])
long = np.array(df[1])

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

IS_AMBULANCE = True

channel.basic_publish(exchange='logs', routing_key='', body="START")

# using datetime module
import datetime;
  
##info
for i in range(len(lat)):
    data = {}
    data["id"] = 0
    ct = str(datetime.datetime.now())
    data["timestamp"] = ct

    if IS_AMBULANCE:
        data["type"] = "AMBULANCE"
        data["start"] = str(lat[0])+"," + str(long[1])
        data["destination"] = str(lat[-1])+"," + str(long[-1])
    else:
        data["type"] = "CAR"

    data["current_location"] = str(lat[i])+"," + str(long[i])
    data["pulse"] = i
    data["flag"] = "none"
    if i == 0:
        data["flag"] = "true"
    elif i == len(lat)-1:
        data["flag"] = "false"     

    json_data = json.dumps(data)
    message = json_data
    channel.basic_publish(exchange='logs', routing_key='', body=message)
    time.sleep(1)
    print(message)

connection.close()




