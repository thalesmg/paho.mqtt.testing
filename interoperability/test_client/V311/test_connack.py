import mqtt.clients.V311 as mqtt_client
import mqtt.formats.MQTTV311 as MQTTV3

from test_client.V311.test_basic import *

import pytest
import functools
import time
import socket

@pytest.fixture(scope="class", autouse=True)
def __cleanup():
    cleanup(["myclientid", "myclientid2"])

class TestConnack():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self, pytestconfig):
        self.host = pytestconfig.getoption('host')
        self.port = int(pytestconfig.getoption('port'))
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
    
    def test_ack_flag(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)
        connack = client.connect(host=self.host, port=self.port, cleansession=False)
        assert connack.flags == 0x00
        client.disconnect()
        connack = client.connect(host=self.host, port=self.port, cleansession=False)
        assert connack.flags == 0x01

    def test_return_code(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid", callback)

        connack = client.connect(host=self.host, port=self.port, cleansession=True)
        assert connack.returnCode == 0
        client.disconnect()

        connack = client.connect(host=self.host, port=self.port, cleansession=True, protocolVersion=6, assertReturnCode=False)
        assert connack.returnCode == 1

        callback.clear()
        client = mqtt_client.Client("", callback)
        connack = client.connect(host=self.host, port=self.port, cleansession=False, assertReturnCode=False)
        assert connack.returnCode == 2

        # ToDo: return code is 3, 4, 5





