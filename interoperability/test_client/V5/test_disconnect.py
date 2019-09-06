from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

@pytest.mark.skip(strict=True, reason='server not supported')
def test_reason_code():
  # [MQTT-3.14.2-1]
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=topics[2], willQoS=1, willMessage=b"will message")

  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(1)])
  waitfor(callback2.subscribeds, 1, 3)

  aclient.disconnect(reasonCode="Disconnect with will message")
  waitfor(callback2.messages, 1, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 1
  assert callback2.messages[0][1] == b"will message"

def test_session_expiry_interval():
  # [MQTT-3.14.2-2]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 999999999
  aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)

  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 999999999
  aclient.disconnect(properties=disconnect_properties)
  connack = aclient.connect(host=host, port=port, cleanstart=False)
  assert connack.sessionPresent == True
  aclient.disconnect()

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 0
  aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)

  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 999999999

  disconnect = MQTTV5.Disconnects()
  disconnect.properties = disconnect_properties
  mqtt_client.main.sendtosocket(aclient.sock, disconnect.pack())

  waitfor(callback.disconnects, 1, 3)
  assert callback.disconnects[0]["reasonCode"].value == 130

def test_disconnect_action():
  # server send disconnect
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 0
  aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)

  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 999999999

  disconnect = MQTTV5.Disconnects()
  disconnect.properties = disconnect_properties
  mqtt_client.main.sendtosocket(aclient.sock, disconnect.pack())

  waitfor(callback.disconnects, 1, 3)
  assert callback.disconnects[0]["reasonCode"].value == 130
  # [MQTT-3.14.4-1] [MQTT-3.14.4-2]
  assert aclient.sock.recv(1024) == b''

  # client send disconnect
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=topics[0], willMessage=b"will message")
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.subscribeds, 1, 3)
  aclient.disconnect()
  waitfor(callback2.messages, 1, 3)
  # [MQTT-3.14.4-3]
  assert len(callback2.messages) == 0
