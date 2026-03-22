#!/usr/bin/env python3

# Test whether a PUBLISH to a topic with QoS 1 results in the correct PUBACK packet.

from mosq_test_helper import *

def do_test(proto_ver):
    rc = 1
    mid = 19
    keepalive = 60
    connect_packet = mosq_test.gen_connect("pub-qos1-test", keepalive=keepalive, proto_ver=proto_ver)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    publish_packet = mosq_test.gen_publish("pub/qos1/test", qos=1, mid=mid, payload="message", proto_ver=proto_ver)
    if proto_ver == 5:
        puback_packet = mosq_test.gen_puback(mid, proto_ver=proto_ver, reason_code=mqtt5_rc.MQTT_RC_NO_MATCHING_SUBSCRIBERS)
    else:
        puback_packet = mosq_test.gen_puback(mid, proto_ver=proto_ver)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
        mosq_test.do_send_receive(sock, publish_packet, puback_packet, "puback")

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
            print("proto_ver=%d" % (proto_ver))
            exit(rc)


do_test(proto_ver=4)
do_test(proto_ver=5)
exit(0)

