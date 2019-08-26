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
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1)
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
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1)
  aclient.disconnect()
  # [MQTT-3.3.1-5] [MQTT-3.3.1-8]
  assert len(callback.messages) == 3
  assert callback.messages[0][1] == b'new qos 0 retained'
  assert callback.messages[1][1] == b'new qos 1 retained'
  assert callback.messages[2][1] == b'new qos 2 retained'

  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[1], b"", 0, retained=True)
  aclient.publish(topics[2], b"", 1, retained=True)
  aclient.publish(topics[3], b"", 2, retained=True)
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1)
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
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  # [MQTT-3.3.1-9] 
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,0)])
  time.sleep(.1)
  bclient.disconnect()
  assert len(callback2.messages) == 3
  # [MQTT-3.3.1-10] 
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,1)])
  time.sleep(.1)
  callback2.clear()
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,1)])
  time.sleep(.1)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  # [MQTT-3.3.1-11]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False,2)])
  time.sleep(.1)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  # Retain As Published
  # [MQTT-3.3.1-12]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,False)])
  time.sleep(.1)
  bclient.disconnect()
  assert not callback2.messages[0][3]
  assert not callback2.messages[1][3]
  assert not callback2.messages[2][3]
  # [MQTT-3.3.1-13]
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2,False,True)])
  time.sleep(.1)
  bclient.disconnect()
  assert callback2.messages[0][3]
  assert callback2.messages[1][3]
  assert callback2.messages[2][3]
  aclient.disconnect()

def test_overlapping_subscriptions():
  # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
  # the server may send back one message with the highest QoS of any matching subscription, or one message for
  # each subscription with a matching QoS.
  # [MQTT-3.3.4-2]
  aclient.connect(host=host, port=port)
  aclient.subscribe([wildtopics[6], wildtopics[0]], [MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(1)])
  aclient.publish(topics[3], b"overlapping topic filters", 2)
  time.sleep(1)
  assert len(callback.messages) in [1, 2]
  if len(callback.messages) == 1:
    # This server is publishing one message for all matching overlapping subscriptions, not one for each.
    assert callback.messages[0][2] == 2
  else:
    # This server is publishing one message per each matching overlapping subscription.
    assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or (callback.messages[0][2] == 1 and callback.messages[1][2] == 2)
  aclient.disconnect()
  cleanup()

  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = "23333"
  aclient.connect(host=host, port=port)
  aclient.subscribe([wildtopics[6], wildtopics[0]], [MQTTV5.SubscribeOptions(2), MQTTV5.SubscribeOptions(1)])

def test_user_properties():
  # [MQTT-3.3.2-17] [MQTT-3.3.2-18]
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(3)
  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.UserProperty = ("a", "2")
  publish_properties.UserProperty = ("c", "3")
  aclient.publish(topics[0], b"", 0, retained=False, properties=publish_properties)
  aclient.publish(topics[0], b"", 1, retained=False, properties=publish_properties)
  aclient.publish(topics[0], b"", 2, retained=False, properties=publish_properties)
  while len(callback.messages) < 3:
    time.sleep(.1)
  aclient.disconnect()
  assert len(callback.messages) == 3
  assert callback.messages[0][5].UserProperty == [("c", "3"), ("a", "2")]
  assert callback.messages[1][5].UserProperty == [("c", "3"), ("a", "2")]
  assert callback.messages[2][5].UserProperty == [("c", "3"), ("a", "2")]
  qoss = [callback.messages[i][2] for i in range(3)]
  assert 1 in qoss and 2 in qoss and 0 in qoss

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

def test_request_response():
  # [MQTT-3.3.2-16]
  callback.clear()
  callback2.clear()

  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)

  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback.subscribeds, 1, 3)

  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback.subscribeds, 1, 3)

  publish_properties = MQTTV5.Properties(MQTTV5.PacketTypes.PUBLISH)
  publish_properties.ResponseTopic = topics[0]
  publish_properties.CorrelationData = b"334"
  # client a is the requester
  aclient.publish(topics[0], b"request", 1, properties=publish_properties)

  # client b is the responder
  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 1
  assert callback2.messages[0][5].ResponseTopic == topics[0]
  assert callback2.messages[0][5].CorrelationData == b"334"

  bclient.publish(callback2.messages[0][5].ResponseTopic, b"response", 1,
                  properties=callback2.messages[0][5])

  # client a gets the response
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][5].ResponseTopic == topics[0]
  assert callback.messages[0][5].CorrelationData == b"334" 

  aclient.disconnect()
  bclient.disconnect()

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
  aclient.publish(topics[0], b"topic alias 1", 1, properties=publish_properties)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1

  aclient.publish("", b"topic alias 2", 1, properties=publish_properties)
  waitfor(callback.messages, 2, 3)
  assert len(callback.messages) == 2
  aclient.disconnect() # should get rid of the topic aliases but not subscriptions