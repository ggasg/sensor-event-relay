from paho.mqtt import client as mqtt
import time
import os
from dotenv import load_dotenv

class MQTTClient:

    # overriding on_publish callback
    def on_publish(self, client, userdata, mid, reason_code, properties):
        try:
            userdata.remove(mid)
        except KeyError:
            print('Chachi messed up just this time')

    def generate_timestamp_element(self):
        return f"\"timestamp\":\"{time.time_ns()}\""
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
        else:
            # we should always subscribe from on_connect callback to be sure
            # our subscribed is persisted across reconnections.
            self.client.subscribe(os.getenv("TEMP_SENSOR_TOPIC"))

    def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        # Since we subscribed only for a single channel, reason_code_list contain a single entry
        if reason_code_list[0].is_failure:
            print(f"Broker rejected you subscription: {reason_code_list[0]}")
        else:
            print(f"Broker granted the following QoS: {reason_code_list[0].value}")

    def on_unsubscribe(self, client, userdata, mid, reason_code_list, properties):
        # Be careful, the reason_code_list is only present in MQTTv5.
        if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
            print("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
        else:
            print(f"Broker replied with failure: {reason_code_list[0]}")
        client.disconnect()
    
    def on_message(self, client, userdata, message):
        # userdata is the structure we choose to provide, here it's a list()
        userdata.append(message.payload)
        # TODO - Doing only 3 messages for the moment
        if len(userdata) >= 3:
            client.unsubscribe(os.getenv("TEMP_SENSOR_TOPIC"))

    def __init__(self) -> None:
        load_dotenv()
        broker = os.getenv("BROKER_HOST")
        self.topic = os.getenv("TEMP_SENSOR_TOPIC")
        self.unacked_publish = set()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_publish = self.on_publish
        self.client.on_connect = self.on_connect
        self.client.on_unsubscribe = self.on_unsubscribe
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.user_data_set([])
        print('Connecting to MQTT...', broker)
        self.client.username = os.getenv("BROKER_USR")
        self.client.password = os.getenv("BROKER_PWD")
        will_message = f"{self.generate_timestamp_element()}, \"Client Disconnect\""
        self.client.will_set(self.topic, will_message,qos=1, retain=False)
        self.client.connect(broker)

    def publish_to_central(self, payload):
        reading_message = f"\"reading\": {payload}"
        msg_info = self.client.publish(self.topic, f"{{{self.generate_timestamp_element()}, {reading_message}}}", qos=1)
        self.unacked_publish.add(msg_info.mid)
        msg_info.wait_for_publish()
