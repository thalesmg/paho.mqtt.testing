from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_reason_string():
  # [MQTT-3.9.2-1]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 128
  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert callback.subscribeds[0][2].ReasonString

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 64
  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert not callback.subscribeds[0][2].ReasonString

# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_user_properties():
  # [MQTT-3.9.2-2]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 128
  aclient.connect(host=host, port=port, properties=connect_properties)
  
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  for i in range(int(connect_properties.MaximumPacketSize / 2)):
    sub_properties.UserProperty = ("a"+str(i),str(i))
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert callback.subscribeds[0][2].UserProperty

  aclient.connect(host=host, port=port, properties=connect_properties)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  for i in range(connect_properties.MaximumPacketSize * 2):
    sub_properties.UserProperty = ("a",str(i))
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect
  assert not callback.subscribeds[0][2].UserProperty

def test_reason_code():
  # [MQTT-3.9.3-1] [MQTT-3.9.3-2]
  aclient.connect(host=host, port=port)
  aclient.subscribe([topics[0], topics[1], topics[2]], [MQTTV5.SubscribeOptions(0), MQTTV5.SubscribeOptions(1), MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert len(callback.subscribeds[0][1]) == 3
  assert callback.subscribeds[0][1][0].value == 0
  assert callback.subscribeds[0][1][1].value == 1
  assert callback.subscribeds[0][1][2].value == 2