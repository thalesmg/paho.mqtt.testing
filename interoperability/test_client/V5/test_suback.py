from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

@pytest.fixture(scope="module", autouse=True)
def __setUp(pytestconfig):
  global host, port
  host = pytestconfig.getoption('host')
  port = int(pytestconfig.getoption('port'))

def test_reason_code():
  # [MQTT-3.9.3-1] [MQTT-3.9.3-2]
  aclient.connect(host=host, port=port)
  aclient.subscribe([topics[0], topics[1], topics[2]], [MQTTV5.SubscribeOptions(0), MQTTV5.SubscribeOptions(1), MQTTV5.SubscribeOptions(2)])
  waitfor(callback.subscribeds, 1, 3)
  aclient.disconnect()
  assert len(callback.subscribeds[0][1]) == 3
  assert callback.subscribeds[0][1][0].value == 0
  assert callback.subscribeds[0][1][1].value == 1
  assert callback.subscribeds[0][1][2].value == 2