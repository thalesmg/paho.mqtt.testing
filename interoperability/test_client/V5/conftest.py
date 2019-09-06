import pytest
import mqtt.clients.V5 as mqtt_client, time, logging, socket, sys, getopt, traceback
import mqtt.formats.MQTTV5 as MQTTV5
from .test_basic import *
from .test_basic import Callbacks

# def pytest_addoption(parser):
#     parser.addoption(
#         "--host", default="localhost"
#     )
#     parser.addoption(
#         "--port", default="1883"
#     )

# @pytest.fixture
# def gethost(request):
#     return request.config.getoption("--host")

# @pytest.fixture
# def getport(request):
#     return request.config.getoption("--port")

@pytest.fixture(scope="session", autouse=True)
def clean():
  cleanup()

@pytest.fixture(scope="function", autouse=True)
def cleanCallback():
  callback.clear()
  callback2.clear()
