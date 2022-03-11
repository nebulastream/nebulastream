import random
import time
import random

from paho.mqtt import client as mqtt_client

broker = 'localhost'
port = 1883
topic = "iris"
client_id = f'python-mqtt-iris'
username = 'emqx'
# password = 'public'

lines = []
with open('/home/sumegim/Documents/tub/thesis/nebulastream/nes-core/tests/test_data/iris.csv', 'r') as f:
    lines = f.readlines()

random.shuffle(lines)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.tls_set()
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_iterator = 0
    dataset_length = len(lines)
    while True:
        time.sleep(0.001)
        msg = f"{lines[msg_iterator].strip()}," + str(time.time_ns()) 
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"topic: {topic} Sending: {msg}")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_iterator += 1
        if(msg_iterator == dataset_length):
            msg_iterator = 0


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()