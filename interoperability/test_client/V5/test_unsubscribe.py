from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, mqtt.clients.V5 as mqtt_client, pytest,time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

@pytest.mark.skip(strict=True, reason='this is a bug')
def test_unsubscribe_payload():
  # [MQTT-3.10.3-1]
  # with pytest.raises(Exception):
  #   aclient.connect(host=host, port=port, cleanstart=True)
  #   aclient.unsubscribe(["取消订阅".encode('gbk')])

  # [MQTT-3.10.3-2]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.unsubscribe([])
  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 130

def test_unsubscribe_actions():
  bclient.connect(host=host, port=port, cleanstart=True)
  # [MQTT-3.10.4-4] [MQTT-3.10.4-5]
  bclient.unsubscribe([topics[4]])
  waitfor(callback2.unsubscribeds, 1, 3)
  assert len(callback2.unsubscribeds) == 1
  assert callback2.unsubscribeds[0][1][0].value == 17

  callback2.clear()
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  bclient.subscribe([topics[1]], [MQTTV5.SubscribeOptions(2)])
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  bclient.subscribe([topics[3]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.subscribeds, 3, 3)
  # [MQTT-3.10.4-6]
  bclient.unsubscribe([topics[0], topics[1]])
  waitfor(callback2.unsubscribeds, 1, 3)
  assert len(callback2.unsubscribeds) == 1

  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[0], b"topic 0 - unsubscribed", 1, retained=False)
  aclient.publish(topics[1], b"topic 1 - unsubscribed", 1, retained=False)
  aclient.publish(topics[2], b"topic 2 - unsubscribed", 1, retained=False)
  aclient.publish(topics[3], b"topic 3 - unsubscribed", 1, retained=False)
  waitfor(callback2.messages, 2, 3)
  bclient.disconnect()
  aclient.disconnect()

  # [MQTT-3.10.4-1]
  assert len(callback2.messages) == 2
  assert callback2.messages[0][0] in [topics[2], topics[3]]
  assert callback2.messages[1][0] in [topics[2], topics[3]]

  