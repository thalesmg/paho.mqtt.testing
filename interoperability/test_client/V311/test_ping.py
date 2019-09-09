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

class TestPing():

    @pytest.fixture(scope="function", autouse=True)
    def __init(self):
        topic_prefix = "client_test3/"
        self.topics = [topic_prefix + topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
        self.wildtopics = [topic_prefix + topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]

    def test_behaviour(self):
        callback = Callbacks()
        client = mqtt_client.Client("myclientid")
        client.connect(host=host, port=port, cleansession=True, keepalive=1)
        spent = 0
        while spent < 3:
            pingreq = MQTTV3.Pingreqs()
            client.sock.send(pingreq.pack())
            packet = MQTTV3.unpackPacket(MQTTV3.getPacket(client.sock))
            assert packet.fh.MessageType == MQTTV3.PINGRESP
            time.sleep(.5)
            spent += 0.5
        time.sleep(3)
        assert b'' == client.sock.recv(1)
