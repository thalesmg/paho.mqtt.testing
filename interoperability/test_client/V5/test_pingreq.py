from .test_basic import * 
import mqtt.formats.MQTTV5 as MQTTV5, time

def test_pingreq():
    aclient.connect(host=host, port=port)
    assert aclient.pingreq()