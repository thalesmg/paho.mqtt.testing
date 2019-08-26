from .test_basic import *
import mqtt.formats.MQTTV5 as MQTTV5, mqtt.clients.V5 as mqtt_client, pytest,time

def test_unsubscribe():
  callback2.clear()
  bclient.connect(host=host, port=port, cleanstart=True)
  bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2)])
  bclient.subscribe([topics[1]], [MQTTV5.SubscribeOptions(2)])
  bclient.subscribe([topics[2]], [MQTTV5.SubscribeOptions(2)])
  time.sleep(1) # wait for any retained messages, hopefully
  # Unsubscribe from one topic
  bclient.unsubscribe([topics[0]])
  callback2.clear() # if there were any retained messsages
  time.sleep(2) # wait for unsubscribe to complete
  
  aclient.connect(host=host, port=port, cleanstart=True)
  aclient.publish(topics[0], b"topic 0 - unsubscribed", 1, retained=False)
  aclient.publish(topics[1], b"topic 1", 1, retained=False)
  aclient.publish(topics[2], b"topic 2", 1, retained=False)
  time.sleep(2)
  
  bclient.disconnect()
  aclient.disconnect()
  assert len(callback2.messages) == 2