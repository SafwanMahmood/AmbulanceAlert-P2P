
import pandas as pd
import numpy as np
import pika
import sys
import time
import json


df = pd.read_csv('ambu-sim1.csv',header=None)

lat = np.array(df[0])
long = np.array(df[1])

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

IS_AMBULANCE = True


data = {}
data['id'] = 0


json_data = json.dumps(data)


if IS_AMBULANCE:
    m_list = []
    for i in range(len(lat)):
        message = str(lat[i]) + "," + str(long[i])
        m_list.append(message)

    "#".join(m_list)    
        



for i in range(len(lat)):
    message = str(lat[i]) + "," + str(long[i])
    channel.basic_publish(exchange='logs', routing_key='', body=message)
    time.sleep(1)
    print("[x] Sent %r" % message)

connection.close()




