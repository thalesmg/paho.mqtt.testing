from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

def test_subscribe_options():
  # [MQTT-3.8.3-3]
  # noLocal
  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback.subscribeds, 1, 3)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])
  waitfor(callback2.subscribeds, 1, 3)
  aclient.publish(topics[0], b"noLocal test", 1, retained=False)
  waitfor(callback2.messages, 1, 3)

  assert len(callback.messages) == 0
  assert len(callback2.messages) == 1
  aclient.disconnect()
  bclient.disconnect()

  callback.clear()
  callback2.clear()

  # retainAsPublished
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, retainAsPublished=True)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"retain as published false", 1, retained=False)
  aclient.publish(topics[0], b"retain as published true", 1, retained=True)

  waitfor(callback.messages, 2, 3)
  time.sleep(1)

  aclient.disconnect()
  assert len(callback.messages) == 2
  assert callback.messages[0][3] == False
  assert callback.messages[1][3] == True

  callback.clear()
  # retainHandling
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[1], b"qos 0", 0, retained=True)
  aclient.publish(topics[2], b"qos 1", 1, retained=True)
  aclient.publish(topics[3], b"qos 2", 2, retained=True)
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=1)])
  time.sleep(1)
  assert len(callback.messages) == 3
  qoss = [callback.messages[i][2] for i in range(3)]
  assert 1 in qoss and 2 in qoss and 0 in qoss
  aclient.disconnect()

  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=2)])
  time.sleep(1)
  assert len(callback.messages) == 0
  aclient.disconnect()

  callback.clear()
  aclient.connect(host=host, port=port, cleanstart=True)
  time.sleep(1)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2, retainHandling=0)])
  time.sleep(1)
  assert len(callback.messages) == 3
  qoss = [callback.messages[i][2] for i in range(3)]
  assert 1 in qoss and 2 in qoss and 0 in qoss
  aclient.disconnect()

  cleanRetained()

def test_subscribe_identifiers():
  callback.clear()
  callback2.clear()

  aclient.connect(host=host, port=port, cleanstart=True)
  sub_properties = MQTTV5.Properties(MQTTV5.PacketTypes.SUBSCRIBE)
  sub_properties.SubscriptionIdentifier = 456789
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

  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][5].SubscriptionIdentifier[0] == 456789
  aclient.disconnect()

  waitfor(callback2.messages, 2, 5)
  assert len(callback2.messages) == 2
  expected_subsids = set([2, 3])
  received_subsids = set([callback2.messages[0][5].SubscriptionIdentifier[0], 
                          callback2.messages[1][5].SubscriptionIdentifier[0]])
  assert received_subsids == expected_subsids
  bclient.disconnect()

def test_shared_subscriptions():
  shared_sub_topic = '$share/sharename/' + topic_prefix + 'x'
  shared_pub_topic = topic_prefix + 'x'

  connack = aclient.connect(host=host, port=port, cleanstart=True)
  assert connack.properties.SharedSubscriptionAvailable == 1
  aclient.subscribe([shared_sub_topic, topics[0]], [MQTTV5.SubscribeOptions(2)]*2) 
  waitfor(callback.subscribeds, 1, 3)

  # [MQTT-3.8.3-4]
  with pytest.raises(Exception):
    bclient.connect(host=host, port=port, cleanstart=True)
    bclient.subscribe(shared_sub_topic, MQTTV5.SubscribeOptions(QoS=2, noLocal=1)) 
    
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([shared_sub_topic, topics[0]], [MQTTV5.SubscribeOptions(2)]*2) 
  waitfor(callback2.subscribeds, 1, 3)

  callback.clear()
  callback2.clear()

  count = 1
  for i in range(count):
    bclient.publish(topics[0], "message "+str(i), 0)
  j = 0
  while len(callback.messages) + len(callback2.messages) < 2*count and j < 20:
    time.sleep(.1)
    j += 1
  time.sleep(1)
  assert len(callback.messages) == count
  assert len(callback2.messages) == count

  callback.clear()
  callback2.clear()

  for i in range(count):
    bclient.publish(shared_pub_topic, "message "+str(i), 0)
  j = 0
  while len(callback.messages) + len(callback2.messages) < count and j < 20:
    time.sleep(.1)
    j += 1
  time.sleep(1)
  # Each message should only be received once
  assert len(callback.messages) + len(callback2.messages) == count

  aclient.disconnect()
  bclient.disconnect()