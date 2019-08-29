from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

def test_retained_message():
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)

  # retained messages
  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  # send a retained message
  aclient.publish(topics[1], b"qos 0 retained", 0, retained=True, properties=publish_properties)
  aclient.publish(topics[2], b"qos 1 retained", 1, retained=True, properties=publish_properties)
  aclient.publish(topics[3], b"qos 2 retained", 2, retained=True, properties=publish_properties)
  waitfor(callback.publisheds, 3, 3)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.messages, 3, 3)
  assert len(callback.messages) == 3
  aclient.unsubscribe([wildtopics[5]])
  callback.clear()
  # send a unretained message
  aclient.publish(topics[1], b"qos 0 not retained", 0, retained=False, properties=publish_properties)
  aclient.publish(topics[2], b"qos 1 not retained", 1, retained=False, properties=publish_properties)
  aclient.publish(topics[3], b"qos 2 not retained", 2, retained=False, properties=publish_properties)
  # send a new retained messag
  aclient.publish(topics[1], b"new qos 0 retained", 0, retained=True, properties=publish_properties)
  aclient.publish(topics[2], b"new qos 1 retained", 1, retained=True, properties=publish_properties)
  aclient.publish(topics[3], b"new qos 2 retained", 2, retained=True, properties=publish_properties)
  waitfor(callback.publisheds, 6, 6)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  # [MQTT-3.3.1-5] [MQTT-3.3.1-8]
  waitfor(callback.messages, 3, 3)
  assert len(callback.messages) == 3
  assert callback.messages[0][1] in [ b'new qos 0 retained', b'new qos 1 retained', b'new qos 2 retained']
  assert callback.messages[1][1] in [ b'new qos 0 retained', b'new qos 1 retained', b'new qos 2 retained']
  assert callback.messages[2][1] in [ b'new qos 0 retained', b'new qos 1 retained', b'new qos 2 retained']

  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[1], b"", 0, retained=True)
  aclient.publish(topics[2], b"", 1, retained=True)
  aclient.publish(topics[3], b"", 2, retained=True)
  waitfor(callback.publisheds, 3, 3)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert len(callback.messages) == 0 # [MQTT-3.3.1-6] 

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  bclient.disconnect()
  assert len(callback2.messages) == 0 # [MQTT-3.3.1-7]

  # Retain Handling Subscription Option
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[1], b"qos 0 retained", 0, retained=True)
  aclient.publish(topics[2], b"qos 1 retained", 1, retained=True)
  aclient.publish(topics[3], b"qos 2 retained", 2, retained=True)
  waitfor(callback.publisheds, 3, 3)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  # [MQTT-3.3.1-9] 
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,0)])
  waitfor(callback2.subscribeds, 1, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 3
  # [MQTT-3.3.1-10] 
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,1)])
  waitfor(callback2.subscribeds, 1, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  # [MQTT-3.3.1-11]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,2)])
  waitfor(callback2.subscribeds, 1, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  # Retain As Published
  # [MQTT-3.3.1-12]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False)])
  waitfor(callback2.subscribeds, 1, 3)
  bclient.disconnect()
  assert not callback2.messages[0][3]
  assert not callback2.messages[1][3]
  assert not callback2.messages[2][3]
  # [MQTT-3.3.1-13]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,True)])
  waitfor(callback2.subscribeds, 1, 3)
  bclient.disconnect()
  assert callback2.messages[0][3]
  assert callback2.messages[1][3]
  assert callback2.messages[2][3]
  aclient.disconnect()

  cleanRetained()
  
# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_topic():
  # [MQTT-3.3.2-1]
  with pytest.raises(Exception):
    aclient.connect(host=host, port=port, cleanstart=True)
    aclient.publish("主题名".encode('gbk'), b"test topic", 1)
  
  # [MQTT-3.3.2-2]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(wildtopics[0], b"test topic", 1)
  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 143

  # [MQTT-3.3.2-3]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.publish(topics[0], b"test topic", 1)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert callback.messages[0][0] == topics[0]

def test_payload_format_indicator():
  # [MQTT-3.3.2-4]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.PayloadFormatIndicator = 56
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  aclient.publish(topics[0], b"test_payload_format_indicator", 1, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "PayloadFormatIndicator")
  assert callback.messages[0][5].PayloadFormatIndicator == publish_properties.PayloadFormatIndicator

def test_message_expiry_interval():
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 99999
  bclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
  disconnect_properties.SessionExpiryInterval = 999999999
  bclient.disconnect(properties = disconnect_properties)

  time.sleep(1)

  aclient.connect(host=host, port=port, cleanstart=True)
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.MessageExpiryInterval = 1
  aclient.publish("topic/A", b"qos 1 - expire", 1, retained=True, properties=publish_properties)
  aclient.publish("topic/B", b"qos 2 - expire", 2, retained=True, properties=publish_properties)
  publish_properties.MessageExpiryInterval = 6
  aclient.publish("topic/C", b"qos 1 - don't expire", 1, retained=True, properties=publish_properties)
  aclient.publish("topic/D", b"qos 2 - don't expire", 2, retained=True, properties=publish_properties)

  time.sleep(3)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe(["topic/+"], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1)
  bclient.disconnect()
  aclient.disconnect()
  # [MQTT-3.3.2-5] 
  assert len(callback2.messages) == 2
  # [MQTT-3.3.2-6]
  assert callback2.messages[0][5].MessageExpiryInterval < 6
  assert callback2.messages[1][5].MessageExpiryInterval < 6

  cleanRetained()

# set mqtt.max_topic_alias = 10 in emqx.conf
def test_client_topic_alias():
  # no server side topic aliases allowed
  # [MQTT-3.3.2-8]
  aclient.connect(host=host, port=port, cleanstart=True)
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = 0 # topic alias 0 not allowed
  aclient.publish(topics[0], "topic alias 0", 1, properties=publish_properties)
  # should get back a disconnect with Topic alias invalid
  waitfor(callback.disconnects, 1, 2)
  assert len(callback.disconnects) == 1

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.TopicAliasMaximum = 0 # server topic aliases not allowed
  connect_properties.SessionExpiryInterval = 99999
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                                        properties=connect_properties)
  assert hasattr(connack.properties, "TopicAliasMaximum")
  if connack.properties.TopicAliasMaximum == 0:
    aclient.disconnect
    return

  callback.clear()
  # [MQTT-3.3.2-9]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = connack.properties.TopicAliasMaximum + 1
  aclient.publish(topics[0], b"Greater than Topic Alias Maximum", 1, properties=publish_properties)
  waitfor(callback.disconnects, 1, 2)
  assert len(callback.disconnects) == 1

  callback.clear()
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                                        properties=connect_properties)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  # [MQTT-3.3.2-12]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.TopicAlias = 1
  aclient.publish(topics[0], b"topic alias 1", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1

  aclient.publish("", b"topic alias 2", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 2, 3)
  assert len(callback.messages) == 2
  aclient.disconnect() # should get rid of the topic aliases but not subscriptions

# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_response_topic():
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  # [MQTT-3.3.2-13]
  with pytest.raises(Exception):
    publish_properties.ResponseTopic = "响应主题".encode('gbk')
    aclient.connect(host=host, port=port, cleanstart=True)
    aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  # [MQTT-3.3.2-14]
  publish_properties.ResponseTopic = wildtopics[0]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.disconnects, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]["reasonCode"].value == 130

  # [MQTT-3.3.2-15]
  publish_properties.ResponseTopic = topics[0]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "ResponseTopic")
  assert callback.messages[0][5].ResponseTopic == publish_properties.ResponseTopic

def test_correlation_data():
  # [MQTT-3.3.2-16]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.CorrelationData = b"2333"

  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_response_topic", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "CorrelationData")
  assert callback.messages[0][5].CorrelationData == publish_properties.CorrelationData

def test_user_properties():
  # [MQTT-3.3.2-17] [MQTT-3.3.2-18]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.UserProperty = ("a", "2")
  publish_properties.UserProperty = ("c", "3")

  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert callback.messages[0][5].UserProperty == [("c", "3"), ("a", "2")]

# @pytest.mark.xfail(strict=True, reason='unconfirmed'
def test_content_type():
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  # [MQTT-3.3.2-19]
  with pytest.raises(Exception):
    publish_properties.ContentType = "内容类型".encode('gbk')
    aclient.connect(host=host, port=port, cleanstart=True)
    aclient.publish(topics[0], b"test_content_type", 1, retained=False, properties=publish_properties)

  # [MQTT-3.3.2-20]
  publish_properties.ContentType = '2333'
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"test_content_type", 1, retained=False, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  aclient.disconnect()
  assert hasattr(callback.messages[0][5], "ContentType")
  assert callback.messages[0][5].ContentType == publish_properties.ContentType


def test_overlapping_subscriptions():
  # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
  # the server may send back one message with the highest QoS of any matching subscription, or one message for
  # each subscription with a matching QoS.
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 2333
  aclient.connect(host=host, port=port)
  aclient.subscribe([wildtopics[6], wildtopics[0]], [MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(1)], properties=sub_properties)
  waitfor(callback.subscribeds, 2, 3)
  aclient.publish(topics[3], b"overlapping topic filters", 2)
  # while aclient.getReceiver().inMsgs != {}:
  #   time.sleep(.1)
  waitfor(callback.publisheds, 1, 3)
  aclient.disconnect()
  # [MQTT-3.3.4-2]
  assert len(callback.messages) in [1, 2]
  if len(callback.messages) == 1:
    # This server is publishing one message for all matching overlapping subscriptions, not one for each.
    assert callback.messages[0][2] == 2
  else:
    # This server is publishing one message per each matching overlapping subscription.
    assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or (callback.messages[0][2] == 1 and callback.messages[1][2] == 2)
  # [MQTT-3.3.4-3]
  for m in callback.messages:
    assert hasattr(m[5], "SubscriptionIdentifier")
    assert m[5].SubscriptionIdentifier == sub_properties.SubscriptionIdentifier

  
def test_subscribe_identifiers():
  aclient.connect(host=host, port=port, cleanstart=True)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 23333
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)
  waitfor(callback.subscribeds, 1, 3)

  bclient.connect(host=host, port=port, cleanstart=True)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 2
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)

  sub_properties.clear()
  sub_properties.SubscriptionIdentifier = 3
  bclient.subscribe([topics[0]+"/#"], [MQTTV5.SubscribeOptions(2)], properties=sub_properties)

  waitfor(callback2.subscribeds, 1, 3)
  bclient.publish(topics[0], b"sub identifier test", 1, retained=False)

  # [MQTT-3.3.4-4]
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][5].SubscriptionIdentifier[0] == 23333
  aclient.disconnect()

  # [MQTT-3.3.4-5]
  waitfor(callback2.messages, 2, 5)
  assert len(callback2.messages) == 2
  expected_subsids = set([2, 3])
  received_subsids = set([callback2.messages[0][5].SubscriptionIdentifier[0], 
                          callback2.messages[1][5].SubscriptionIdentifier[0]])
  assert received_subsids == expected_subsids
  bclient.disconnect()

  # [MQTT-3.3.4-6]
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.SubscriptionIdentifier = 2333

  aclient.connect(host=host, port=port)
  with pytest.raises(Exception):
    aclient.publish(topics[0], b"test_subscription_identifier", 1, retained=False, properties=publish_properties)