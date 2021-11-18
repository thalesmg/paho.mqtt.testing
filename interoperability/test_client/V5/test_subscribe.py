from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, time

# These need to be imported explicitly so that pytest sees it
from .test_basic import base_socket_timeout, base_sleep, base_wait_for


@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_subscribe():
  # [MQTT-3.8.3-1]
  # with pytest.raises(Exception):
  #   aclient.connect(host=host, port=port, cleanstart=True)
  #   aclient.subscribe(["订阅主题".encode("gbk")], [MQTTV5.SubscribeOptions(2)])

  # [MQTT-3.8.3-2]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.disconnects, 1, 2)
  assert callback.disconnects[0]["reasonCode"].value == 143

@pytest.mark.rlog_flaky
def test_subscribe_options(base_wait_for, base_sleep):
  # [MQTT-3.8.3-3]
  # noLocal
  callback.clear()
  callback2.clear()

  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback.subscribeds, 1, 3 * base_wait_for)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback2.subscribeds, 1, 3 * base_wait_for)

  aclient.publish(topics[0], b"noLocal test", 1, retained=False)
  waitfor(callback.messages, 1, 3 * base_wait_for)
  waitfor(callback2.messages, 1, 3 * base_wait_for)

  assert len(callback.messages) == 0
  assert len(callback2.messages) == 1
  aclient.disconnect()
  bclient.disconnect()

  callback.clear()
  callback2.clear()

  # retainAsPublished
  pubclient = mqtt_client.Client("pubclient".encode("utf-8"))
  pubclient.connect(host=host, port=port, cleanstart=True)

  pubclient.publish(topics[1], b"This is a server reserved message", 1, retained=True)
  pubclient.publish(topics[3], b"This is another server reserved message", 1, retained=True)
  time.sleep(2 * base_sleep)

  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([wildtopics[0]], [MQTTV5.SubscribeOptions(2, retainAsPublished=True)])
  bclient.subscribe([wildtopics[0]], [MQTTV5.SubscribeOptions(2, retainAsPublished=False)])
  waitfor(callback.messages, 2, 5 * base_wait_for)
  waitfor(callback2.messages, 2, 5 * base_wait_for)

  assert callback.messages[0][3] == True
  assert callback.messages[1][3] == True
  assert callback2.messages[0][3] == True
  assert callback2.messages[1][3] == True
  callback.clear()
  callback2.clear()

  pubclient.publish(topics[1], b"retained true", 1, retained=True)

  waitfor(callback.messages, 1, 5 * base_wait_for)
  waitfor(callback2.messages, 1, 5 * base_wait_for)
  assert callback.messages[0][3] == True
  assert callback2.messages[0][3] == False

  aclient.disconnect()
  bclient.disconnect()
  callback.clear()
  callback2.clear()
  cleanRetained(host, port)

  # retainHandling
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[1], b"qos 0", 0, retained=True)
  aclient.publish(topics[2], b"qos 1", 1, retained=True)
  aclient.publish(topics[3], b"qos 2", 2, retained=True)
  waitfor(callback.publisheds, 2, 3 * base_wait_for)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=1)])
  waitfor(callback.messages, 3, 3 * base_wait_for)
  assert len(callback.messages) == 3
  qoss = [callback.messages[i][2] for i in range(3)]
  assert 1 in qoss and 2 in qoss and 0 in qoss
  aclient.disconnect()

  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=2)])
  waitfor(callback.messages, 1, 3 * base_wait_for)
  assert len(callback.messages) == 0
  aclient.disconnect()

  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=0)])
  waitfor(callback.messages, 3, 3 * base_wait_for)
  assert len(callback.messages) == 3
  qoss = [callback.messages[i][2] for i in range(3)]
  assert 1 in qoss and 2 in qoss and 0 in qoss
  aclient.disconnect()

  cleanRetained(host, port)

def test_subscribe_actions():
  # [MQTT-3.8.4-1] [MQTT-3.8.4-2]
  aclient.connect(host=host, port=port, cleanstart=True)
  packet_id = aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert callback.subscribeds[0][0] == packet_id

  # [MQTT-3.8.4-3] [MQTT-3.8.4-4]
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[0], b"test_subscribe_actions: retain should be true", 2, retained=True)
  waitfor(callback.publisheds, 1, 3)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(QoS=2, retainAsPublished=1, retainHandling=0)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[0]
  assert callback.messages[0][1] == b'test_subscribe_actions: retain should be true'
  assert callback.messages[0][2] == 2
  assert callback.messages[0][3] == True
  callback.clear()

  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(QoS=1, retainAsPublished=0, retainHandling=0)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[0]
  assert callback.messages[0][1] == b'test_subscribe_actions: retain should be true'
  assert callback.messages[0][2] == 1
  assert callback.messages[0][3] == True
  callback.clear()

  aclient.publish(topics[0], b"test_subscribe_actions: retain should be flase", 2, retained=True)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][0] == topics[0]
  assert callback.messages[0][1] == b'test_subscribe_actions: retain should be flase'
  assert callback.messages[0][2] == 1
  assert callback.messages[0][3] == False
  callback.clear()

  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(QoS=2, retainHandling=2)])
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 0
  aclient.disconnect()
  cleanRetained(host, port)

  # [MQTT-3.8.4-5]
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0],topics[1],topics[2]], [MQTTV5.SubscribeOptions(2),MQTTV5.SubscribeOptions(2),MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.unsubscribe([topics[0],topics[1],topics[2]])
  aclient.disconnect()
  assert len(callback.subscribeds) == 1

  # [MQTT-3.8.4-6] [MQTT-3.8.4-7] [MQTT-3.8.4-8]
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(0)])
  waitfor(callback.subscribeds, 1, 3)
  assert callback.subscribeds[0][1][0].value == 0
  callback.clear()
  aclient.subscribe([topics[1]], [MQTTV5.SubscribeOptions(1)])
  waitfor(callback.subscribeds, 1, 3)
  assert callback.subscribeds[0][1][0].value == 1
  callback.clear()
  aclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  assert callback.subscribeds[0][1][0].value == 2
