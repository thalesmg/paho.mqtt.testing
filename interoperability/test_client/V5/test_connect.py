from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, mqtt.clients.V5 as mqtt_client, pytest,time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))
  cleanup(host, port)

def test_basic():
  aclient.connect(host=host, port=port)
  aclient.disconnect()

  rc = aclient.connect(host=host, port=port)
  assert rc.reasonCode.getName() == "Success"
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.publish(topics[0], b"qos 0")
  aclient.publish(topics[0], b"qos 1", 1)
  aclient.publish(topics[0], b"qos 2", 2)
  waitfor(callback.messages, 3, 3)
  assert len(callback.messages) == 3
  aclient.disconnect()

def test_protocol():
  with pytest.raises(Exception):
    aclient.connect(host=host, port=port)
    aclient.connect(host=host, port=port, newsocket=False) # should fail - second connect on socket [MQTT-3.1.0-2]

  # [MQTT-3.1.2-1]
  connack = aclient.connect(host=host, port=port, protocolName="hj")
  assert connack.reasonCode.value == 132

  ## server not supported
  ## [MQTT-3.1.2-2]
  # connect = MQTTV5.Connects()
  # connect.ProtocolName = "MQTT"
  # connect.ProtocolVersion = 6
  # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  # sock.settimeout(.5)
  # sock.connect((host, port))
  # mqtt_client.main.sendtosocket(sock, connect.pack())
  # response = MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  # assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK
  # assert response.reasonCode.value == 129

def test_clean_start():
  # [MQTT-3.1.2-4]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 5
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  assert connack.sessionPresent == False
  # [MQTT-3.1.2-5]
  aclient.terminate()
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  assert connack.sessionPresent == True
  aclient.disconnect
  # [MQTT-3.1.2-6]
  connack = bclient.connect(host=host, port=port, cleanstart=False)
  assert connack.sessionPresent == False
  bclient.disconnect()

def test_will_message():
  # will properties
  will_properties = MQTTV5.Properties(MQTTV5.PacketTypes.WILLMESSAGE)
  will_properties.WillDelayInterval = 0 # this is the default anyway
  will_properties.UserProperty = ("a", "2")
  will_properties.UserProperty = ("c", "3")
  
  # [MQTT-3.1.2-7] [MQTT-3.1.2-8]
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=topics[2], willMessage=b"will message", keepalive=2,
      willProperties=will_properties)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.subscribeds, 1, 3)
  # keep alive timeout ought to be triggered so the will message is received
  waitfor(callback2.messages, 1, 10)
  bclient.disconnect()
  aclient.disconnect()
  assert len(callback2.messages) == 1
  assert callback2.messages[0][1] == b"will message"
  assert callback2.messages[0][5].UserProperty ==  [('c', '3'), ('a', '2')] or callback2.messages[0][5].UserProperty ==  [('a', '2'), ('c', '3')]
  
  # [MQTT-3.1.2-9]
  with pytest.raises(Exception): 
    aclient.connect(host=host, port=port, cleanstart=True, willFlag=True)

  # [MQTT-3.1.2-10]
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
    willTopic=topics[2], willMessage=b"will message", 
    willProperties=will_properties)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.subscribeds, 1, 3)
  aclient.disconnect()
  waitfor(callback2.messages, 1, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 0
 
def test_will_qos():
  # [MQTT-3.1.2-11]
  # with pytest.raises(Exception):
  #   connect = MQTTV5.Connects()
  #   connect.ClientIdentifier = "testWillQos"
  #   connect.CleanStart = True
  #   connect.KeepAliveTimer = 0
  #   connect.WillFlag = False
  #   connect.WillTopic = topics[2]
  #   connect.WillMessage = "testWillQos"
  #   connect.WillQoS = 2
  #   connect.WillRETAIN = 0
  #   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #   sock.settimeout(.5)
  #   sock.connect((host, port))
  #   mqtt_client.main.sendtosocket(sock, connect.pack())
  #   MQTTV5.unpackPacket(MQTTV5.getPacket(sock))

  # [MQTT-3.1.2-12]
  will_properties = MQTTV5.Properties(MQTTV5.PacketTypes.WILLMESSAGE)
  will_properties.WillDelayInterval = 0 # this is the default anyway

  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
    willTopic=topics[2], willMessage=b"will message", willQoS=0,keepalive=2,
    willProperties=will_properties)
  aclient.disconnect
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
    willTopic=topics[2], willMessage=b"will message", willQoS=1,keepalive=2,
    willProperties=will_properties)
  aclient.disconnect
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
    willTopic=topics[2], willMessage=b"will message", willQoS=2,keepalive=2,
    willProperties=will_properties)
  aclient.disconnect

  cleanRetained(host, port)

def test_will_retain():
  # [MQTT-3.1.2-13]
  # with pytest.raises(Exception):
  #   connect = MQTTV5.Connects()
  #   connect.ClientIdentifier = "testWillRetain"
  #   connect.CleanStart = True
  #   connect.KeepAliveTimer = 0
  #   connect.WillFlag = False
  #   connect.WillTopic = topics[2]
  #   connect.WillMessage = "testWillRetain"
  #   connect.WillQoS = 0
  #   connect.WillRETAIN = 1
  #   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #   sock.settimeout(.5)
  #   sock.connect((host, port))
  #   mqtt_client.main.sendtosocket(sock, connect.pack())
  #   MQTTV5.unpackPacket(MQTTV5.getPacket(sock))

  # [MQTT-3.1.2-14]
  will_properties = MQTTV5.Properties(MQTTV5.PacketTypes.WILLMESSAGE)
  will_properties.WillDelayInterval = 0 # this is the default anyway
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=topics[2], willMessage=b"will message", willRetain=0,
      keepalive=2, willProperties=will_properties)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.subscribeds, 1, 3)
  # keep alive timeout ought to be triggered so the will message is received
  waitfor(callback2.messages, 1, 10)
  bclient.disconnect()
  aclient.disconnect()
  assert len(callback2.messages) == 1
  assert not callback2.messages[0][3]

  # [MQTT-3.1.2-15]
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=topics[2], willMessage=b"will message", willRetain=1,
      keepalive=2, willProperties=will_properties)
  bclient.connect(host=host, port=port, cleanstart=False)
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2,False,True)])
  waitfor(callback2.subscribeds, 1, 3)
  # keep alive timeout ought to be triggered so the will message is received
  waitfor(callback2.messages, 1, 10)
  bclient.disconnect()
  aclient.disconnect()
  assert len(callback2.messages) == 1
  assert callback2.messages[0][3]

  cleanRetained(host, port)

@pytest.mark.skip(strict=True, reason='server not supported')
def test_username_flag():
  # [MQTT-3.1.2-16]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testUsernameFlag"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.usernameFlag = False
    connect.username = "testUsernameFlag"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, connect.pack())
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  
  # [MQTT-3.1.2-17]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testUsernameFlag"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.usernameFlag = True
    connect.username = None
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, connect.pack())
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))

@pytest.mark.skip(strict=True, reason='server not supported')
def test_password_flag():
  # [MQTT-3.1.2-18]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testPasswordFlag"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.passwordFlag = False
    connect.password = "testPasswordFlag"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, connect.pack())
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))
  
  # [MQTT-3.1.2-19]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testPasswordFlag"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.passwordFlag = True
    connect.password = None
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, connect.pack())
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))  

def test_keepalive():
  # keepalive processing.  We should be kicked off by the server if we don't send or receive any data, and don't send any pings either.
  keepalive = 1
  
  bclient.connect(host=host, port=port, cleanstart=True, keepalive=3)
  bclient.subscribe([topics[4]], [MQTTV5.SubscribeOptions(2)])
  
  connack = aclient.connect(host=host, port=port, cleanstart=True, keepalive=keepalive, willFlag=True,
        willTopic=topics[4], willQoS=1, willMessage=b"keepalive expiry")

  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False
  # [MQTT-3.1.2-21]
  if hasattr(connack.properties, "ServerKeepAlive"):
    keepalive = connack.properties.ServerKeepAlive

  # [MQTT-3.1.2-20]
  time.sleep(int(keepalive))
  aclient.pingreq()
  assert len(callback2.messages) == 0

  # [MQTT-3.1.2-22]
  time.sleep(int(keepalive*1.5)) # Keepalive timeout
  waitfor(callback2.messages,1,3)
  bclient.disconnect()
  assert len(callback2.messages) == 1 and callback2.messages[0][1] == b"keepalive expiry"  # should have the will message

def test_offline_message_queueing():
  # [MQTT-3.1.2-23]
  callback.clear()
  callback2.clear()

  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 99999
  aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties)
  aclient.subscribe([wildtopics[5]], [MQTTV5.SubscribeOptions(2)])
  aclient.disconnect()

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.publish(topics[1], b"qos 0", 0)
  bclient.publish(topics[2], b"qos 1", 1)
  bclient.publish(topics[3], b"qos 2", 2)
  waitfor(callback2.publisheds, 2, 3)
  bclient.disconnect()

  aclient.connect(host=host, port=port, cleanstart=False)
  waitfor(callback.messages, 2, 3)
  aclient.disconnect()

  assert len(callback.messages) in [2, 3]

def test_redelivery_on_reconnect():
  # redelivery on reconnect. When a QoS 1 or 2 exchange has not been completed, the server should retry the appropriate MQTT packets
  # [MQTT-3.1.2-23]
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.SessionExpiryInterval = 99999
  bclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  bclient.subscribe([wildtopics[6]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback2.messages, 1, 3)
  bclient.pause() # stops responding to incoming publishes
  bclient.publish(topics[1], b"", 1, retained=False)
  bclient.publish(topics[3], b"", 2, retained=False)
  waitfor(callback2.publisheds, 2, 3)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  bclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties)
  waitfor(callback2.messages, 2, 3)
  assert len(callback2.messages) == 2
  bclient.disconnect()

def test_maximum_packet_size():
  # [MQTT-3.1.2-24]
  maximumPacketSize = 256 # max packet size we want to receive
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)
  connect_properties.MaximumPacketSize = maximumPacketSize
  connack = aclient.connect(host=host, port=port, cleanstart=True,
                                          properties=connect_properties)

  assert hasattr(connack.properties, "MaximumPacketSize")

  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)

  # send a small enough packet, should get this one back
  payload = b"."*(int(maximumPacketSize/2))
  aclient.publish(topics[0], payload, 0)
  waitfor(callback.messages, 1, 3)
  assert len(callback.messages) == 1

  callback.clear()
  # send a packet too big to receive
  payload = b"."*(int(maximumPacketSize*2))
  aclient.publish(topics[0], payload, 1)
  waitfor(callback.messages, 2, 3)
  assert len(callback.messages) == 0

  aclient.disconnect()

def test_clientid():
  # [MQTT-3.1.3-3]
  with pytest.raises(Exception):
    client = mqtt_client.Client(None)
    connack = client.connect(host=host, port=port)
    assert connack.properties.AssignedClientIdentifie != ""
  
  # [MQTT-3.1.3-4]
  # with pytest.raises(Exception):
  #   client = mqtt_client.Client("客户端".encode('gbk'))
  #   client.connect(host=host, port=port)

  # [MQTT-3.1.3-5]
  client = mqtt_client.Client("这是cliend_id 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".encode("utf-8"))
  client.connect(host=host, port=port)
  client.disconnect()
  
  # [MQTT-3.1.3-6] [MQTT-3.1.3-7]
  client0 = mqtt_client.Client("")
  connack = client0.connect(host=host, port=port, cleanstart=True) # should not be rejected
  assert connack.reasonCode.value == 0 and connack.sessionPresent == False
  client0.disconnect()
  connack = client0.connect(host=host, port=port, cleanstart=False)
  assert connack.reasonCode.value == 133

def test_will_delay():
  will_properties = MQTTV5.Properties(MQTTV5.PacketTypes.WILLMESSAGE)
  connect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.CONNECT)

  will_properties.WillDelayInterval = 6 # in seconds
  connect_properties.SessionExpiryInterval = 10
  
  # [MQTT-3.1.3-9]
  callback.clear()
  callback2.clear()
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties,
        willProperties=will_properties, willFlag=True, willTopic=topics[0], willMessage=b"test_will_delay will message")
  assert connack.sessionPresent == False

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)]) # subscribe to will message topic
  waitfor(callback2.subscribeds, 1, 3)
  aclient.terminate()
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties,
        willProperties=will_properties, willFlag=True, willTopic=topics[0], willMessage=b"test_will_delay will message")
  assert connack.sessionPresent == True
  
  waitfor(callback2.messages, 1, will_properties.WillDelayInterval)
  assert len(callback2.messages) == 0
  aclient.disconnect()
  bclient.disconnect()

  # if session expiry is less than will delay then session expiry is used
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties,
        willProperties=will_properties, willFlag=True, willTopic=topics[0], willMessage=b"test_will_delay will message")
  assert connack.sessionPresent == False

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)]) # subscribe to will message topic
  waitfor(callback2.subscribeds, 1, 3)

  aclient.terminate()
  time.sleep(will_properties.WillDelayInterval)
  connack = aclient.connect(host=host, port=port, cleanstart=False, properties=connect_properties,
      willProperties=will_properties, willFlag=True, willTopic=topics[0], willMessage=b"test_will_delay will message")
  assert connack.sessionPresent == True

  waitfor(callback2.messages, 1, will_properties.WillDelayInterval)
  bclient.disconnect()
  assert callback2.messages[0][0] == topics[0]
  assert callback2.messages[0][1] == b"test_will_delay will message"

  aclient.disconnect()
  bclient.disconnect()

  # if session expiry is less than will delay then session expiry is used
  will_properties.WillDelayInterval = 5 # in seconds
  connect_properties.SessionExpiryInterval = 3
  callback.clear()
  callback2.clear()
  connack = aclient.connect(host=host, port=port, cleanstart=True, properties=connect_properties,
        willProperties=will_properties, willFlag=True, willTopic=topics[0], willMessage=b"test_will_delay will message")
  assert connack.sessionPresent == False

  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)]) # subscribe to will message topic
  waitfor(callback2.subscribeds, 1, 3)
  aclient.terminate()
  waitfor(callback2.messages, 1, connect_properties.SessionExpiryInterval)
  bclient.disconnect()
  assert callback2.messages[0][0] == topics[0]
  assert callback2.messages[0][1] == b"test_will_delay will message"

@pytest.mark.skip(strict=True, reason='server not supported')
def test_will_topic():
  connack = aclient.connect(host=host, port=port, cleanstart=True, willFlag=True,
      willTopic=wildtopics[0], willMessage=b"will message")
  assert connack.reasonCode.value == 144

  # [MQTT-3.1.3-11]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testWillTopic"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.WillFlag = True
    connect.WillTopic = "遗嘱主题".encode("gbk")       # UTF-8
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, connect.pack())
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))  

@pytest.mark.skip(strict=True, reason='server not supported')
def test_username():
  # [MQTT-3.1.3-12]
  with pytest.raises(Exception):
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "testUsername"
    connect.CleanStart = True
    connect.KeepAliveTimer = 0
    connect.usernameFlag = True
    connect.username = "用户名".encode("gbk")

    connectFlags = bytes([(connect.CleanStart << 1) | (connect.WillFlag << 2) | \
                       (connect.WillQoS << 3) | (connect.WillRETAIN << 5) | \
                       (connect.usernameFlag << 6) | (connect.passwordFlag << 7)])
    buffer = MQTTV5.writeUTF(connect.ProtocolName) + bytes([connect.ProtocolVersion]) + \
              connectFlags + MQTTV5.writeInt16(connect.KeepAliveTimer)
    buffer += connect.properties.pack()
    buffer += MQTTV5.writeUTF(connect.ClientIdentifier)
    buffer += bytes(connect.username)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(.5)
    sock.connect((host, port))
    mqtt_client.main.sendtosocket(sock, buffer)
    MQTTV5.unpackPacket(MQTTV5.getPacket(sock))  

def test_connect_actions():
  aclient.connect(host=host, port=port, willFlag=True, willTopic=topics[0], willMessage=b"test_connect_actions")
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)]) # subscribe to will message topic
  waitfor(callback2.subscribeds, 1, 3)

  new_client = mqtt_client.Client("myclientid".encode("utf-8"))
  connack = new_client.connect(host=host, port=port, cleanstart=True)
  assert connack.reasonCode.getName() == "Success" and connack.sessionPresent == False# [MQTT-3.1.4-4]  [MQTT-3.1.4-5]
  waitfor(callback.disconnects, 1, 3)
  waitfor(callback2.messages, 1, 3)
  assert len(callback.disconnects) == 1
  assert callback.disconnects[0]['reasonCode'].value == 142
  ## server not supported
  ## [MQTT-3.1.4-3]
  # assert len(callback2.messages) == 1 
  # assert callback2.messages[0][0] == topics[0]
  # assert callback2.messages[0][1] == b"test_connect_actions"

  # [MQTT-3.1.4-6]
  aclient.connect(host=host, port=port, protocolName="hj")
  aclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  assert len(callback.subscribeds) == 0 