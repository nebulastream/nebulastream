#!/usr/bin/env python3

# Test whether the broker handles malformed packets correctly - SUBSCRIBE
# MQTTv5

from mosq_test_helper import *

rc = 1

def do_test(subscribe_packet, reason_code, error_string):
    global rc

    rc = 1

    keepalive = 10
    connect_packet = mosq_test.gen_connect("test", proto_ver=5, keepalive=keepalive)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    mid = 0
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=reason_code)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
    mosq_test.do_send_receive(sock, subscribe_packet, disconnect_packet, error_string=error_string)
    rc = 0


port = mosq_test.get_port()
broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

try:
    # mid == 0
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=1, mid=0, proto_ver=5)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "mid == 0")

    # qos > 2
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=3, mid=1, proto_ver=5)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "qos > 2")

    # retain handling = 0x30
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=0x30, mid=1, proto_ver=5)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "retain handling = 0x30")

    # subscription options = 0xC0
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=0xC0, mid=1, proto_ver=5)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "subscription options = 0xC0")

    # command flags != 0x02
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=1, mid=1, proto_ver=5, cmd=128)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "command flags != 0x02")

    # Incorrect property
    props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_SESSION_EXPIRY_INTERVAL, 0)
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=1, mid=1, proto_ver=5, properties=props)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Incorrect property")

    # Truncated packet, no mid
    subscribe_packet = struct.pack("!BB", 130, 0)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, no mid")

    # Truncated packet, no properties
    subscribe_packet = struct.pack("!BBH", 130, 2, 1)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, no properties")

    # Truncated packet, with properties field
    subscribe_packet = struct.pack("!BBHB", 130, 3, 1, 0)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field")

    # Truncated packet, with properties field, empty topic
    subscribe_packet = struct.pack("!BBHBH", 130, 5, 1, 0, 0)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, empty topic")

    # Truncated packet, with properties field, empty topic, with qos
    subscribe_packet = struct.pack("!BBHBHB", 130, 6, 1, 0, 0, 1)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, empty topic, with qos")

    # Truncated packet, with properties field, with topic, no qos
    subscribe_packet = struct.pack("!BBHBH1s", 130, 6, 1, 0, 1, b"a")
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, with topic, no qos")

    # Truncated packet, with properties field, with 1st topic and qos ok, second topic ok, no second qos
    subscribe_packet = struct.pack("!BBHHH1sBH1s", 130, 10, 1, 0, 1, b"a", 0, 1, b"b")
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, with 1st topic and qos ok, second topic ok, no second qos")

    # Bad topic
    subscribe_packet = mosq_test.gen_subscribe(topic="#/test/topic", qos=1, mid=1, proto_ver=5)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Bad topic")

    # Subscription ID set to 0
    props = mqtt5_props.gen_varint_prop(mqtt5_props.PROP_SUBSCRIPTION_IDENTIFIER, 0)
    subscribe_packet = mosq_test.gen_subscribe(topic="test/topic", qos=1, mid=1, proto_ver=5, properties=props)
    do_test(subscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Subscription ID set to 0")
except mosq_test.TestError:
    pass
finally:
    broker.terminate()
    broker.wait()
    (stdo, stde) = broker.communicate()
    if rc:
        print(stde.decode('utf-8'))
        exit(rc)
