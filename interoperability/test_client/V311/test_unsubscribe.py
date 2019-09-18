import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3

from test_client.V311.test_basic import *

import pytest
import functools
import time
import socket
import sys

@pytest.fixture(scope="class", autouse=True)
def __cleanup(pytestconfig):
    global host, port
    host = pytestconfig.getoption('host')
    port = int(pytestconfig.getoption('port'))
    cleanup(["myclientid", "myclientid2"], host=host, port=port)

class TestUnsubscribe():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self):
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]

    # [MQTT-3.10.3-2]
    def test_no_payload(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=host, port=port, cleansession=True)
        unsubscribe = MQTTV3.Unsubscribes()
        unsubscribe.messageIdentifier = 1
        client.sock.send(unsubscribe.pack())
        assert b'' == client.sock.recv(1)

    def test_behaviour(self):
        # [MQTT-3.10.4.1], [MQTT-3.10.4.2], [MQTT-3.10.4-4]
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=host, port=port, cleansession=True)
        client.subscribe([self.wildtopics[0]], [0])
        waitfor(callback.subscribeds, 1, 1)
        client.publish(self.topics[1], b"test behaviour: subscribe")
        waitfor(callback.messages, 1, 1)
        callback.clear()
        client.unsubscribe([self.wildtopics[0]])
        waitfor(callback.unsubscribeds, 1, 1)
        client.publish(self.topics[1], b"test behaviour: unsubscribe")
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 1, 1)
        client.disconnect()
        callback.clear()

        # [MQTT-3.10.4-3]
        # Set zone.external.retry_interval = 2s in emqx.conf
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        client.connect(host=host, port=port, cleansession=True)
        client.subscribe([self.wildtopics[0]], [2])
        waitfor(callback.subscribeds, 1, 1)
        client.pause() # stops responding to incoming publishes
        client.publish(self.topics[1], b"test behaviour: will not be lost qos=1", qos=1)
        client.publish(self.topics[1], b"test behaviour: will not be lost qos=2", qos=2)
        waitfor(callback.publisheds, 2, 2)
        client.unsubscribe([self.wildtopics[0]])
        waitfor(callback.unsubscribeds, 1, 1)
        with pytest.raises(Exception) as e:
            waitfor(callback.messages, 2, 4)
        client.resume()
        waitfor(callback.messages, 2, 4)
        client.disconnect()
        callback.clear()

        # [MQTT-3.10.4-4], [MQTT-3.10.4-5]
        client = mqtt_client.Client("myclientid")
        client.connect(host=host, port=port, cleansession=True)
        unsubscribe = MQTTV3.Unsubscribes()
        unsubscribe.data= self.topics[0]
        unsubscribe.messageIdentifier = 1
        client.sock.send(unsubscribe.pack())
        unsuback = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
        assert unsuback.fh.MessageType == MQTTV3.UNSUBACK
        assert unsuback.messageIdentifier == unsubscribe.messageIdentifier


