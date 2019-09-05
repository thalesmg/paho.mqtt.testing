import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3

from test_client.V311.test_basic import *

import pytest
import functools
import time
import socket
import sys

@pytest.fixture(scope="function", autouse=True)
def __cleanup():
    cleanup(["myclientid", "myclientid2"])

class TestSubscribe():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self, pytestconfig):
        self.host = pytestconfig.getoption('host')
        self.port = int(pytestconfig.getoption('port'))
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]

    def test_behaviour(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.subscribe([self.wildtopics[0]], [0])
        waitfor(callback.subscribeds, 1, 1)
        client.publish(self.topics[1], b"test subscribe behaviour 1")
        client.publish(self.topics[3], b"test subscribe behaviour 2")
        waitfor(callback.messages, 2, 1)
        client.disconnect()
        callback.clear()

    # [MQTT-3.8.3-3]
    def test_no_payload(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=self.host, port=self.port, cleansession=True)
        subscribe = MQTTV3.Subscribes()
        subscribe.messageIdentifier = 1
        client.sock.send(subscribe.pack())
        assert b'' == client.sock.recv(1)

    # [MQTT-3.8.4-1], [MQTT-3.8.4-2]
    def test_suback(self):
        client = mqtt_client.Client("myclientid")
        client.connect(host=self.host, port=self.port, cleansession=True)
        subscribe = MQTTV3.Subscribes()
        subscribe.data.append((self.topics[0], 0))
        subscribe.messageIdentifier = 1
        client.sock.send(subscribe.pack())
        suback = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert suback.fh.MessageType == MQTTV3.SUBACK
        assert suback.messageIdentifier == subscribe.messageIdentifier

    # [MQTT-3.8.4-3], [MQTT-3.8.4-6]
    def test_update_subscription(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.publish(self.topics[0], b"qos should be 1", qos = 2, retained=True)
        client.subscribe([self.topics[0]], [0])
        waitfor(callback.subscribeds, 1, 1)
        assert callback.subscribeds[0][1][0] == 0
        callback.clear()
        client.subscribe([self.topics[0]], [1])
        waitfor(callback.subscribeds, 1, 1)
        assert callback.subscribeds[0][1][0] == 1
        waitfor(callback.messages, 1, 1)
        # QoS should be 1
        assert callback.messages[0][2] == 1
        # Retained should be True
        assert callback.messages[0][3] == True
        client.disconnect()
        callback.clear()

    # [MQTT-3.8.4-4], [MQTT-3.8.4-5], [MQTT-3.9.3-1]
    def test_multiple_topics(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.subscribe([self.topics[0], self.topics[1], self.topics[2]], [0, 1, 2])
        waitfor(callback.subscribeds, 1, 1)
        assert callback.subscribeds[0][1] == [0, 1, 2]
        client.disconnect()
        callback.clear()

    def test_topic_name_and_filters(self):
        # [MQTT-4.7.1.1]
        client = mqtt_client.Client("myclientid")
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.subscribe([self.wildtopics[0]], [0])
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.SUBACK
        client.publish(self.wildtopics[0], b"The wildcard characters MUST NOT be used within a Topic Name")
        assert b'' == client.sock.recv(1)

        # Multi-level wildcard
        client = mqtt_client.Client("myclientid")
        client.connect(host=self.host, port=self.port, cleansession=True)
        # Valid topics
        client.subscribe(["#", "A/#"], [0, 0])
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.SUBACK
        # Invalid topics
        client.subscribe(["A/#/B"], [0])
        assert b'' == client.sock.recv(1)
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.subscribe(["A/B#"], [0])
        assert b'' == client.sock.recv(1)

        # Single level wildcard
        client = mqtt_client.Client("myclientid")
        client.connect(host=self.host, port=self.port, cleansession=True)
        # Valid topics
        client.subscribe(["+", "+/A/+", "A/+/B"], [0, 0, 0])
        packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert packet.fh.MessageType == MQTTV3.SUBACK
        # Invalid topics
        client.subscribe(["A+"], [0])
        assert b'' == client.sock.recv(1)

        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=self.host, port=self.port, cleansession=True)
        client.subscribe(["A/#"], [0])
        waitfor(callback.subscribeds, 1, 1)
        client.publish("A", b"topic A")
        client.publish("A/B", b"topic A/B")
        client.publish("A/B/C", b"topic A/B/C")
        waitfor(callback.messages, 3, 3)
        client.unsubscribe(["A/#"])
        callback.clear()

        client.subscribe(["A/+"], [0])
        waitfor(callback.subscribeds, 1, 1)
        client.publish("A", b"topic A")
        client.publish("A/B", b"topic A/B")
        client.publish("A/B/C", b"topic A/B/C")
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 3, 3)
        assert len(callback.messages) == 1
        client.unsubscribe(["A/#"])
        callback.clear()

        client.subscribe(["#", "+/b"], [0, 0])
        waitfor(callback.subscribeds, 1, 1)
        client.publish("$a/b", b"topic $a/b")
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 1, 1)
        assert len(callback.messages) == 0


