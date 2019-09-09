import pytest

import mqtt.clients.V5 as mqtt_client, time, logging, socket, sys, getopt, traceback
import mqtt.formats.MQTTV5 as MQTTV5

class Callbacks(mqtt_client.Callback):

  def __init__(self):
    self.messages = []
    self.messagedicts = []
    self.publisheds = []
    self.subscribeds = []
    self.unsubscribeds = []
    self.disconnects = []

  def __str__(self):
     return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
        str(self.subscribeds) + str(self.unsubscribeds) + str(self.disconnects)

  def clear(self):
    self.__init__()

  def disconnected(self, reasoncode, properties):
    logging.info("disconnected %s %s", str(reasoncode), str(properties))
    self.disconnects.append({"reasonCode" : reasoncode, "properties" : properties})

  def connectionLost(self, cause):
    logging.info("connectionLost %s" % str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid, properties=None):
    logging.info("publishArrived %s %s %d %s %d %s", topicName, payload, qos, retained, msgid, str(properties))
    self.messages.append((topicName, payload, qos, retained, msgid, properties))
    self.messagedicts.append({"topicname" : topicName, "payload" : payload,
        "qos" : qos, "retained" : retained, "msgid" : msgid, "properties" : properties})
    return True

  def published(self, msgid, reasonCode=None, properties=None):
    logging.info("published %d", msgid)
    self.publisheds.append((msgid, reasonCode, properties))

  def subscribed(self, msgid, reasonCodes, properties):
    logging.info("subscribed %d", msgid)
    self.subscribeds.append((msgid, reasonCodes, properties))

  def unsubscribed(self, msgid, reasonCodes, properties):
    logging.info("unsubscribed %d", msgid)
    self.unsubscribeds.append((msgid, reasonCodes, properties))

callback = Callbacks()
callback2 = Callbacks()
aclient = mqtt_client.Client("myclientid".encode("utf-8"))
aclient.registerCallback(callback)
bclient = mqtt_client.Client("myclientid2".encode("utf-8"))
bclient.registerCallback(callback2)

topic_prefix = "client_test5/"
topics = [topic_prefix+topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
wildtopics = [topic_prefix+topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
denytopics = [topic_prefix+topic for topic in ["TopicD"]]
nosubscribe_topics = ("test/nosubscribe",)

@pytest.fixture(scope="session", autouse=True)
def __setUp(pytestconfig):
  global host1, port1, host2, port2
  host1 = pytestconfig.getoption('host1')
  port1 = int(pytestconfig.getoption('port1'))
  cleanup(host1, port1)
  cleanRetained(host1, port1)

  host2 = pytestconfig.getoption('host2')
  port2 = int(pytestconfig.getoption('port2'))
  cleanup(host2, port2)
  cleanRetained(host2, port2)

@pytest.fixture(scope="function", autouse=True)
def callbackClear():
  callback.clear()
  callback2.clear()

def cleanup(host, port):
    print("clean up starting")
    clientids = ("myclientid", "myclientid2")
    for clientid in clientids:
        curclient = mqtt_client.Client(clientid.encode("utf-8"))
        curclient.connect(host=host, port=port, cleanstart=True)
        time.sleep(.1)
        curclient.disconnect()
        time.sleep(.1)
    
def cleanRetained(host, port):
  callback = Callbacks()
  curclient = mqtt_client.Client("clean retained".encode("utf-8"))
  curclient.registerCallback(callback)
  curclient.connect(host=host, port=port, cleanstart=True)
  curclient.subscribe(["#"], [MQTTV5.SubscribeOptions(0)])
  time.sleep(2) # wait for all retained messages to arrive
  for message in callback.messages:
    logging.info("deleting retained message for topic %s", message[0])
    curclient.publish(message[0], b"", 1, retained=True)
    waitfor(callback.publisheds, 1, 3)
    assert len(callback.publisheds) == 1
    callback.clear()
  curclient.disconnect()
  time.sleep(.1)

def waitfor(queue, depth, limit):
  total = 0
  while len(queue) < depth and total < limit:
    interval = .5
    total += interval
    time.sleep(interval)

def test_basic():
  rc1 = aclient.connect(host=host1, port=port1)
  assert rc1.reasonCode.getName() == "Success"

  rc2 = bclient.connect(host=host2, port=port2)
  assert rc2.reasonCode.getName() == "Success"

  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)

  bclient.publish(topics[0], b"qos 0")
  bclient.publish(topics[0], b"qos 1", 1)
  bclient.publish(topics[0], b"qos 2", 2)
  waitfor(callback2.publisheds, 2, 3)

  waitfor(callback.messages, 3, 3)
  assert len(callback.messages) == 3
  aclient.disconnect()
  bclient.disconnect()