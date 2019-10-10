from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

shared_sub_topic = '$share/sharename/' + topic_prefix + 'x'
shared_pub_topic = topic_prefix + 'x'

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_shared_subscriptions():
  connack = aclient.connect(host=host, port=port, cleanstart=True)
  assert connack.properties.SharedSubscriptionAvailable == 1
  aclient.subscribe([shared_sub_topic, topics[0]], [MQTTV5.SubscribeOptions(2)]*2) 
  waitfor(callback.subscribeds, 1, 3)

  ## this is ia bug
  ## Setting a non-local option to 1 when sharing a subscription will result in a protocol error (protocol error)
  # [MQTT-3.8.3-4]
  # bclient.connect(host=host, port=port, cleanstart=True)
  # bclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(QoS=2, noLocal=1)])
  # waitfor(callback2.disconnects, 1, 3)
  # assert len(callback2.disconnects) == 1
  # assert callback2.disconnects[0]["reasonCode"].value == 130
    
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

@pytest.mark.skip(reason='This is a bug')
def test_retain():
  ## On the first subscription, the message is reserved and not sent to the session.

  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)
  
  aclient.publish(shared_pub_topic, b"test_shared_subscriptions_retain", 1, retained=True)
  bclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)]) 
  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 0
  cleanRetained(host, port)

def test_qos():
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)]) 

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.publish(shared_pub_topic, b"test_shared_subscriptions_qos", 2)

  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][2] == 2

  aclient.disconnect()
  callback.clear()

  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(1)]) 
  bclient.publish(shared_pub_topic, b"test_shared_subscriptions_qos", 2)

  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][2] == 1

@pytest.mark.skip(reason='This is a bug')
def test_client_terminates_when_qos_eq_1():
  ## If the Server is in the process of sending a QoS 1 message to its chosen subscribing Client and the connection to that Client breaks before the Server has received an acknowledgement from the Client, the Server MAY wait for the Client to reconnect and retransmit the message to that Client. If the Client'sSession terminates before the Client reconnects, the Server SHOULD send the Application Message to another Client that is subscribed to the same Shared Subscription. It MAY attempt to send the message to another Client as soon as it loses its connection to the first Client.
  pubclient = mqtt_client.Client("pubclient".encode("utf-8"))
  pubclient.connect(host=host, port=port, cleanstart=True)

  aclient.connect(host=host, port=port, cleanstart=True)

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 5

  connect = MQTTV5.Connects()
  connect.ClientIdentifier = "test_shared_subscriptions"
  connect.CleanStart = True
  connect.properties = connect_properties
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.settimeout(.5)
  sock.connect((host, port))
  mqtt_client.main.sendtosocket(sock, connect.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK

  subscribe = MQTTV5.Subscribes()
  subscribe.packetIdentifier = 1
  subscribe.data.append((shared_sub_topic, MQTTV5.SubscribeOptions(2)))
  mqtt_client.main.sendtosocket(sock, subscribe.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.SUBACK

  pubclient.publish(shared_pub_topic, b"test_shared_subscriptions_client_terminates_when_qos_eq_1", 1)

  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.PUBLISH
  sock.shutdown(socket.SHUT_RDWR)
  sock.close()
  time.sleep(connect_properties.SessionExpiryInterval)
  aclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)])

  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1
  assert callback.messages[0][1] == b'test_shared_subscriptions_client_terminates_when_qos_eq_2'

def test_client_terminates_when_qos_eq_2():
  ##   If the Server is in the process of sending a QoS 2 message to its chosen subscribing Client and the connection to the Client breaks before delivery is complete, the Server MUST complete the delivery of the message to that Client when it reconnects [MQTT-4.8.2-4] as described in section 4.3.3. If the Client's Session terminates before the Client reconnects, the Server MUST NOT send the Application Message to any other subscribed Client [MQTT-4.8.2-5].
  pubclient = mqtt_client.Client("pubclient".encode("utf-8"))
  pubclient.connect(host=host, port=port, cleanstart=True)

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 10

  connect = MQTTV5.Connects()
  connect.ClientIdentifier = "test_shared_subscriptions"
  connect.CleanStart = True
  connect.properties = connect_properties
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.settimeout(.5)
  sock.connect((host, port))
  mqtt_client.main.sendtosocket(sock, connect.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK

  subscribe = MQTTV5.Subscribes()
  subscribe.packetIdentifier = 2
  subscribe.data.append((shared_sub_topic, MQTTV5.SubscribeOptions(2)))
  mqtt_client.main.sendtosocket(sock, subscribe.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.SUBACK

  pubclient.publish(shared_pub_topic, b"test_shared_subscriptions_client_terminates_when_qos_eq_2", 2)

  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.PUBLISH
  sock.shutdown(socket.SHUT_RDWR)
  sock.close()

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)]) 

  connect = MQTTV5.Connects()
  connect.ClientIdentifier = "test_shared_subscriptions"
  connect.CleanStart = False
  connect.properties = connect_properties
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.settimeout(.5)
  sock.connect((host, port))
  mqtt_client.main.sendtosocket(sock, connect.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK
  assert response.sessionPresent == True


  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 0

  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.PUBLISH
  assert response.fh.QoS == 2
  assert response.data == b'test_shared_subscriptions_client_terminates_when_qos_eq_2'

def test_puback_filed():
  ## If a Client responds with a PUBACK or PUBREC containing a Reason Code of 0x80 or greater to a PUBLISH packet from the Server, the Server MUST discard the Application Message and not attempt to send it to any other Subscriber [MQTT-4.8.2-6].
  pubclient = mqtt_client.Client("pubclient".encode("utf-8"))
  pubclient.connect(host=host, port=port, cleanstart=True)

  connect = MQTTV5.Connects()
  connect.ClientIdentifier = "test_shared_subscriptions"
  connect.CleanStart = True
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.settimeout(.5)
  sock.connect((host, port))
  mqtt_client.main.sendtosocket(sock, connect.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK

  subscribe = MQTTV5.Subscribes()
  subscribe.packetIdentifier = 2
  subscribe.data.append((shared_sub_topic, MQTTV5.SubscribeOptions(2)))
  mqtt_client.main.sendtosocket(sock, subscribe.pack())
  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.SUBACK

  pubclient.publish(shared_pub_topic, b"test_puback_filed", 1)

  response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  assert response.fh.PacketType == MQTTV5.PacketTypes.PUBLISH
  assert response.fh.QoS == 1

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)]) 

  puback = MQTTV5.Pubacks()
  puback.packetIdentifier = response.packetIdentifier
  puback.reasonCode = MQTTV5.ReasonCodes(packetType=4, aName="Implementation specific error")
  mqtt_client.main.sendtosocket(sock, puback.pack())

  waitfor(callback2.messages, 1, 3)
  assert len(callback2.messages) == 0

def test_overlapping_subscription():
  ##  A Client is permitted to submit a second SUBSCRIBE request to a Shared Subscription on a Session that's already subscribed to that Shared Subscription. For example, it might do this to change the Requested QoS for its subscription or because it was uncertain that the previous subscribe completed before the previous connection was closed. This does not increase the number of times that the Session is associated with the Shared Subscription, so the Session will leave the Shared Subscription on its first UNSUBSCRIBE.
  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)

  aclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(2)]) 
  waitfor(callback.subscribeds, 1, 3)
  assert callback.subscribeds[0][1][0].value == 2

  callback.clear()

  aclient.subscribe([shared_sub_topic], [MQTTV5.SubscribeOptions(1)]) 
  waitfor(callback.subscribeds, 1, 3)
  assert callback.subscribeds[0][1][0].value == 1

  bclient.publish(shared_pub_topic, b"test_overlapping_subscription_1", 1)
  
  waitfor(callback.messages, 1, 3)
  assert callback.messages[0][1] == b'test_overlapping_subscription_1'
  assert callback.messages[0][2] == 1

@pytest.mark.skip(reason='This is a bug')
def test_subscriptions_to_both_shared_and_non_shared():
  ## Each Shared Subscription is independent from any other. It is possible to have two Shared Subscriptions with overlapping filters. In such cases a message that matches both Shared Subscriptions will be processed separately by both of them. If a Client has a Shared Subscription and a Non‑shared Subscription and a message matches both of them, the Client will receive a copy of the message by virtue of it having the Non‑shared Subscription. A second copy of the message will be delivered to one of the subscribers to the Shared Subscription, and this could result in a second copy being sent to this Client.
  aclient.connect(host=host, port=port, cleanstart=True)
  bclient.connect(host=host, port=port, cleanstart=True)
  
  aclient.subscribe([shared_pub_topic, shared_sub_topic], [MQTTV5.SubscribeOptions(2)]*2)
  bclient.publish(shared_pub_topic, b"test_overlapping_subscription_2", 1)
  waitfor(callback.messages, 2, 3)

  assert len(callback.messages) == 2