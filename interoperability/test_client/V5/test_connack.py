from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_session_present():
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 5
  # [MQTT-3.2.2-2]
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  # [MQTT-3.2.2-3]
  connack = aclient.connect(host=host, port=port, cleanstart=False)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True

def test_session_expiry():
  # no session expiry property == never expire
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 0
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.disconnect()

  # session should immediately expire
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

  connect_properties.SessionExpiryInterval = 5
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.disconnect()

  time.sleep(2)
  # session should still exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True
  aclient.disconnect()

  time.sleep(6)
  # session should not exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

  connect_properties.SessionExpiryInterval = 1
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 5
  aclient.disconnect(properties = disconnect_properties)

  time.sleep(3)
  # session should still exist
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == True
  disconnect_properties.SessionExpiryInterval = 0
  aclient.disconnect(properties = disconnect_properties)

  # session should immediately expire
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  aclient.disconnect()

def test_assigned_cliend_id():
  # [MQTT-3.2.2-16]
  client = mqtt_client.Client("")
  connack = client.connect(host=host, port=port)
  assert connack.reasonCode.getName() == "Success"
  assert hasattr(connack.properties, "AssignedClientIdentifier") and connack.properties.AssignedClientIdentifier != ''
  client.disconnect()

def test_maximum_packet_size():
  # [MQTT-3.2.2-15]
  # 1. server max packet size
  connack = aclient.connect(host=host, port=port, cleanstart=True)
  serverMaximumPacketSize = 2**28-1
  if hasattr(connack.properties, "MaximumPacketSize"):
    serverMaximumPacketSize = connack.properties.MaximumPacketSize + 1

  payload = b"."*serverMaximumPacketSize
  aclient.publish(topics[0], payload, 0)
  # should get back a disconnect with packet size too big
  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert str(callback.disconnects[0]["reasonCode"]) == "Packet too large"
