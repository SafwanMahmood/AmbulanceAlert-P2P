# PeerBase
Go through to understand the Report.md gist/documentation. 
## Reference:

For a **tutorial** see: http://cs.berry.edu/~nhamid/p2p

Also: <br>
Nadeem Abdul Hamid. 2007. **A lightweight framework for peer-to-peer programming.** *J. Comput. Sci. Coll.* 22, 5 (May 2007), 98–104.
https://dl.acm.org/doi/10.5555/1229688.1229706

## Requirements

mysql-connector-python    8.0.29
mysql-connector-python-rf 2.2.2
pika==1.2.1


## Python

(Written for Python 2.x.) can also be run on python3.  

To run the demo Python application:
```
python filergui.py 9001 10 localhost:9000
```

In general,

```
filergui.py <server-port> <max-peers> <peer-ip>:<port>
```
Senors run by running:

Docker rabbitmq:

docker run --rm -it --hostname my-rabbit -p 15672:15672 -p 5672:5672 rabbitmq:3-management

Then:

python sensorambulance.py 
