#!/usr/bin/env python3

# Test whether the broker handles malformed packets correctly - UNSUBSCRIBE
# MQTTv5

from mosq_test_helper import *

rc = 1

def do_test(unsubscribe_packet, reason_code, error_string):
    global rc

    rc = 1

    keepalive = 10
    connect_packet = mosq_test.gen_connect("test", proto_ver=5, keepalive=keepalive)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    mid = 0
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=reason_code)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
    mosq_test.do_send_receive(sock, unsubscribe_packet, disconnect_packet, error_string=error_string)
    rc = 0


port = mosq_test.get_port()
broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

try:
    # mid == 0
    unsubscribe_packet = mosq_test.gen_unsubscribe(topic="test/topic", mid=0, proto_ver=5)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "mid == 0")

    # command flags != 0x02
    unsubscribe_packet = mosq_test.gen_unsubscribe(topic="test/topic", mid=1, proto_ver=5, cmd=160)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "command flags != 0x02")

    # Incorrect property
    props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_SESSION_EXPIRY_INTERVAL, 0)
    unsubscribe_packet = mosq_test.gen_unsubscribe(topic="test/topic", mid=1, proto_ver=5, properties=props)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Incorrect property")

    # Truncated packet, no mid
    unsubscribe_packet = struct.pack("!BB", 162, 0)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, no mid")

    # Truncated packet, no properties
    unsubscribe_packet = struct.pack("!BBH", 162, 2, 1)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, no properties")

    # Truncated packet, with properties field, no topic
    unsubscribe_packet = struct.pack("!BBHH", 162, 4, 1, 0)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, no topic")

    # Truncated packet, with properties field, empty topic
    unsubscribe_packet = struct.pack("!BBHHH", 162, 5, 1, 0, 0)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Truncated packet, with properties field, empty topic")

    # Bad topic
    unsubscribe_packet = mosq_test.gen_unsubscribe(topic="#/test/topic", mid=1, proto_ver=5)
    do_test(unsubscribe_packet, mqtt5_rc.MQTT_RC_MALFORMED_PACKET, "Bad topic")
except mosq_test.TestError:
    pass
finally:
    broker.terminate()
    broker.wait()
    (stdo, stde) = broker.communicate()
    if rc:
        print(stde.decode('utf-8'))
        exit(rc)
