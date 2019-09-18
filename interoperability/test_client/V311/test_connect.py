import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3

from test_client.V311.test_basic import *

import pytest
import functools
import time
import socket

@pytest.fixture(scope="class", autouse=True)
def __cleanup(pytestconfig):
    global host, port
    host = pytestconfig.getoption('host')
    port = int(pytestconfig.getoption('port'))
    cleanup(["myclientid", "myclientid2"], host=host, port=port)

class TestConnect():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self):
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
    
    def test_basic_behavior(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        connack = client.connect(host=host, port=port)
        client.subscribe([self.topics[0]], [2])
        waitfor(callback.subscribeds, 1, 1)
        client.publish(self.topics[0], b"qos 0")
        client.publish(self.topics[0], b"qos 1", 1)
        client.publish(self.topics[0], b"qos 2", 2)
        waitfor(callback.messages, 3, 2)
        client.disconnect()

    # [MQTT-3.1.0-2] Second CONNECT Packet
    def test_second_connect(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=host, port=port)
        with pytest.raises(Exception) as e:
            client.connect(host=host, port=port, newsocket=False)
        assert e.value.args[0] == 'connect failed - socket closed, no connack'

    # [MQTT-3.1.2-1] Incorrect protocol name
    @pytest.mark.skip(reason='unconfirmed')
    def test_proto_name(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        with pytest.raises(Exception) as e:
            client.connect(host=host, port=port, protocolName="bad_name")
        assert e.value.args[0] == 'connect failed - socket closed, no connack'
        # connack = client.connect(host=host, port=port, protocolName="bad_name")
        # assert connack.returnCode == 1

    # [MQTT-3.1.2-2] Unacceptable protocol level
    @pytest.mark.skip(strict=True, reason='Emqx defaults to V5 error handling when protocol version error occurs')
    def test_proto_level(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        connack = client.connect(host=host, port=port, protocolVersion=6, assertReturnCode=False)
        assert connack.returnCode == 1

    def test_clean_session(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        # [MQTT-3.1.2-4], [MQTT-3.2.2-3]
        connack = client.connect(host=host, port=port, cleansession=False)
        assert connack.flags == 0x00
        client.subscribe([self.topics[0]], [2])
        time.sleep(0.5)
        client.pause() # stops responding to incoming publishes
        client.publish(self.topics[0], b"qos 1", 1)
        client.publish(self.topics[0], b"qos 2", 2)
        time.sleep(.5)
        client.disconnect()
        assert len(callback.messages) == 0
        callback.clear()

        # [MQTT-3.2.2-2]
        client.resume()
        connack = client.connect(host=host, port=port, cleansession=False)
        assert connack.flags == 0x01
        waitfor(callback.messages, 2, 2)
        assert callback.messages[0][0] == callback.messages[1][0] == 'client_test3/TopicA'
        client.disconnect()
        callback.clear()

        # [MQTT-3.1.2-6], [MQTT-3.2.2-1]
        connack = client.connect(host=host, port=port, cleansession=True)
        assert connack.flags == 0x00
        client.publish(self.topics[0], b"qos 0")
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 1, 1)
        client.disconnect()
        callback.clear()

        # [MQTT-3.1.2-7]
        connack = client.connect(host=host, port=port, cleansession=True)
        assert connack.flags == 0x00
        client.publish(self.topics[0], b"retain message", qos=0, retained=True)
        client.disconnect()
        callback.clear()

        connack = client.connect(host=host, port=port, cleansession=True)
        assert connack.flags == 0x00
        client.subscribe([self.topics[0]], [0])
        waitfor(callback.messages, 1, 2)
        client.publish(self.topics[0], b"", 0, retained=True)
        time.sleep(.2)
        client.disconnect()
        callback.clear()

    def test_will_message_1(self):
        callback = Callbacks()
        callback2 = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client2 = mqtt_client.Client("myclientid2", callback2)

        # The will message should be published if the client closes the network connection without first sending a DISCONNECT packet
        client.connect(host=host, port=port, cleansession=True,
                       willFlag=True, willTopic=self.topics[0], willQoS=0, willMessage=b"this is a will message")
        client2.connect(host=host, port=port, cleansession=True)
        client2.subscribe([self.topics[0]], [0])
        time.sleep(.1)
        client.terminate()
        waitfor(callback2.messages, 1, 1)
        assert callback2.messages[0][1] == "this is a will message".encode("utf-8")
        client2.disconnect()
        callback.clear()
        callback2.clear()

    def test_will_message_2(self):
        callback = Callbacks()
        callback2 = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client2 = mqtt_client.Client("myclientid2", callback2)

        # The will message should be published if the client fails to communicate within the keep alive time
        # , [MQTT-3.1.2-17]
        client.connect(host=host, port=port, cleansession=True, keepalive=1,
                       willFlag=True, willTopic=self.topics[0], willQoS=0, willRetain=True, willMessage=b"this is a will retain message")
        client2.connect(host=host, port=port, cleansession=True)
        client2.subscribe([self.topics[0]], [0])
        # Wait for the server to disconnect, and the client will receive the will message
        waitfor(callback2.messages, 1, 3)
        assert callback2.messages[0][3] == False

        client2.unsubscribe([self.topics[0]])
        callback2.clear()
        client2.subscribe([self.topics[0]], [0])
        waitfor(callback2.messages, 1, 1)
        assert callback2.messages[0][1] == "this is a will retain message".encode("utf-8")
        assert callback2.messages[0][3] == True

        client2.disconnect()
        callback.clear()
        callback2.clear()

    # def test_will_message_3(self):
        # The will message should be published if the server closes the network connection because of a protocol error
        # ToDo

    def test_keep_alive(self):
        client = mqtt_client.Client("myclientid")
        client.connect(host=host, port=port, cleansession=True, keepalive=1)
        time.sleep(3)
        assert b'' == client.sock.recv(1)

    def test_zero_length_clientid(self):
        callback = Callbacks()
        client = mqtt_client.Client("", callback)
        client.connect(host=host, port=port, cleansession=True)
        client.disconnect()

    # [MQTT-3.1.4-2]
    def test_same_clientid(self):
        callback = Callbacks()
        callback2 = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client2 = mqtt_client.Client("myclientid", callback2)
        
        client.connect(host=host, port=port, cleansession=True)
        client2.connect(host=host, port=port, cleansession=True)
        assert b'' == client.sock.recv(1)





