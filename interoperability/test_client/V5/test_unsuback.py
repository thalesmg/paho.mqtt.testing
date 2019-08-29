from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_reason_string():
  # [MQTT-3.11.2-1]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 128

  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.unsubscribe([topics[0]])
  waitfor(callback.unsubscribeds, 1, 3)
  aclient.disconnect()
  assert callback.subscribeds[0][2].ReasonString

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 64

  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.unsubscribe([topics[0]])
  waitfor(callback.unsubscribeds, 1, 3)
  aclient.disconnect()
  assert not callback.subscribeds[0][2].ReasonString


# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_user_properties():
  # [MQTT-3.11.2-2]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = 128

  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  unsub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.UNSUBSCRIBE)
  for i in range(int(connect_properties.MaximumPacketSize / 2)):
    unsub_properties.UserProperty = ("a"+str(i),str(i))
  aclient.unsubscribe([topics[0]], unsub_properties)
  waitfor(callback.unsubscribeds, 1, 3)
  aclient.disconnect()
  assert callback.subscribeds[0][2].UserProperty

  aclient.connect(host=host, port=port, properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  unsub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.UNSUBSCRIBE)
  for i in range(int(connect_properties.MaximumPacketSize * 2)):
    unsub_properties.UserProperty = ("a"+str(i),str(i))
  aclient.unsubscribe([topics[0]], unsub_properties)
  waitfor(callback.unsubscribeds, 1, 3)
  aclient.disconnect()
  assert not callback.subscribeds[0][2].UserProperty

def test_reason_code():
  # [MQTT-3.11.3-1] [MQTT-3.11.3-2]
  aclient.connect(host=host, port=port)
  aclient.subscribe([topics[0], topics[1], topics[2]], [MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(2)])
  aclient.unsubscribe([topics[0], topics[1], topics[2]])
  waitfor(callback.unsubscribeds, 1, 3)
  aclient.disconnect()
  assert len(callback.unsubscribeds[0][1]) == 3
  assert callback.unsubscribeds[0][1][0].value == 0
  assert callback.unsubscribeds[0][1][1].value == 0
  assert callback.unsubscribeds[0][1][2].value == 0