from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_reason_code():
  # [MQTT-3.4.2-1]
  aclient.connect(host=host, port=port)
  aclient.publish(topics[0], b'test_reason_code', 1)
  waitfor(callback.publisheds, 1, 3)
  assert not callback.publisheds[0][1].value == None
