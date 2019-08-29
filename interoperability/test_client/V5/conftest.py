import pytest
import mqtt.clients.V5 as mqtt_client, time, logging, socket, sys, getopt, traceback
import mqtt.formats.MQTTV5 as MQTTV5
from .test_basic import *
from .test_basic import Callbacks

@pytest.fixture(scope="session", autouse=True)
def clean():
  cleanup()

@pytest.fixture(scope="function", autouse=True)
def cleanCallback():
  callback.clear()
  callback2.clear()
