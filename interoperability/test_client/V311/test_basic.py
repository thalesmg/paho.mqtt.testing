import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3
import logging
import functools
import time
import pytest

class Callbacks(mqtt_client.Callback):

    def __init__(self):
        self.messages = []
        self.publisheds = []
        self.subscribeds = []
        self.unsubscribeds = []

    def clear(self):
        self.__init__()

    def connectionLost(self, cause):
        logging.info("connectionLost %s", str(cause))

    def publishArrived(self, topicName, payload, qos, retained, msgid):
        logging.info("publishArrived %s %s %d %d %d", topicName, payload, qos, retained, msgid)
        self.messages.append((topicName, payload, qos, retained, msgid))
        return True

    def published(self, msgid):
        logging.info("published %d", msgid)
        self.publisheds.append(msgid)

    def subscribed(self, msgid, data):
        logging.info("subscribed %d", msgid)
        self.subscribeds.append((msgid, data))

    def unsubscribed(self, msgid):
        logging.info("unsubscribed %d", msgid)
        self.unsubscribeds.append(msgid)

def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kw):
        print('{0} begin'.format(func.__name__))
        ret = func(*args, **kw)
        print('{0} end'.format(func.__name__))
        return ret
    return wrapper

def cleanup(clientids, host="localhost", port=1883):
    for clientid in clientids:
        curclient = mqtt_client.Client(clientid.encode("utf-8"))
        curclient.connect(host=host, port=port, cleansession=True)
        time.sleep(.1)
        curclient.disconnect()
        time.sleep(.1)

    callback = Callbacks()
    curclient = mqtt_client.Client("clean retained".encode("utf-8"))
    curclient.registerCallback(callback)
    curclient.connect(host=host, port=port, cleansession=True)
    curclient.subscribe(["#"], [0])
    # wait for all retained messages to arrive
    time.sleep(2)
    for message in callback.messages:
        # retained flag
        if message[3]:
            print("deleting retained message for topic", message[0])
            curclient.publish(message[0], b"", 0, retained=True)
    curclient.disconnect()
    time.sleep(.1)

def waitfor(queue, num, duration):
    spent = 0
    interval = .5
    while len(queue) < num and spent < duration:
        spent += interval
        time.sleep(interval)
    assert len(queue) == num

__all__ = ['Callbacks', 'cleanup', 'waitfor']
