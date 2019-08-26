from .test_basic import Callbacks
import mqtt.clients.V5 as mqtt_client

callback = Callbacks()
callback2 = Callbacks()
aclient = mqtt_client.Client("myclientid".encode("utf-8"))
aclient.registerCallback(callback)
bclient = mqtt_client.Client("myclientid2".encode("utf-8"))
bclient.registerCallback(callback2)

topic_prefix = "client_test5/"
topics = [topic_prefix+topic for topic in ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA"]]
wildtopics = [topic_prefix+topic for topic in ["TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#"]]
nosubscribe_topics = ("test/nosubscribe",)
host = "localhost"
port = 1883