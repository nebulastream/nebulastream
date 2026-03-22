#!/usr/bin/env python3

# Test whether "topic alias" works to the broker
# MQTT v5

from mosq_test_helper import *

def do_test():
    rc = 1
    keepalive = 60
    connect_packet = mosq_test.gen_connect("sub-test", keepalive=keepalive, proto_ver=5)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    props = mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS, 3)
    publish1_packet = mosq_test.gen_publish("", qos=0, payload="message", proto_ver=5, properties=props)

    disconnect_packet = mosq_test.gen_disconnect(reason_code=mqtt5_rc.MQTT_RC_PROTOCOL_ERROR, proto_ver=5)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=5, port=port)
        sock.send(publish1_packet)

        mosq_test.expect_packet(sock, "disconnect", disconnect_packet)
        rc = 0

        sock.close()
    except mosq_test.TestError:
        pass
    finally:
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)


do_test()
exit(0)
