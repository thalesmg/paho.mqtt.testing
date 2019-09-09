from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_reason_code():
  # [MQTT-3.4.2-1]
  aclient.connect(host=host, port=port)
  aclient.publish(topics[0], b'test_reason_code', 1)
  waitfor(callback.publisheds, 1, 3)
  assert not callback.publisheds[0][1].value == None

@pytest.mark.skip(strict=True, reason='server not supported')
def test_reason_string():
  # [MQTT-3.4.2-2]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 64
  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.publish(topics[0], b'test_reason_string', 1)
  waitfor(callback.publisheds, 1, 3)
  assert len(callback.publisheds) == 1
  assert callback.publisheds[0][2].ReasonString

  callback.clear
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  for i in range(int(connect_properties * 2)):
    publish_properties.UserProperty = ("a" + str(i), str(i))
  aclient.publish(topics[0], b'test_reason_string', 1, properties=publish_properties)
  waitfor(callback.publisheds, 1, 3)
  assert len(callback.publisheds) == 1
  assert not callback.publisheds[0][2].ReasonString

@pytest.mark.skip(strict=True, reason='server not supported')
def test_user_properties():
  # [MQTT-3.4.2-3]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 64
  aclient.connect(host=host, port=port, properties=connect_properties)

  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  for i in range(int(connect_properties.MaximumPacketSize  / 2)):
    publish_properties.UserProperty = ("a" + str(i), str(i))
  aclient.publish(topics[0], b'test_user_properties', 1, properties=publish_properties)
  waitfor(callback.publisheds, 1, 3)
  assert len(callback.publisheds) == 1
  assert callback.publisheds[0][2].UserProperty

  callback.clear
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  for i in range(int(connect_properties.MaximumPacketSize * 2)):
    publish_properties.UserProperty = ("a" + str(i), str(i))
  aclient.publish(topics[0], b'test_user_properties', 1, properties=publish_properties)
  waitfor(callback.publisheds, 1, 3)
  assert len(callback.publisheds) == 1
  assert not callback.publisheds[0][2].UserProperty


  