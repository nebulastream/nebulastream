#!/usr/bin/env python3

# Test whether a client subscribed to a topic receives its own message sent to that topic.
# MQTT v5

from mosq_test_helper import *

def do_test(proto_ver):
    if proto_ver == 4:
        exit(0)

    rc = 1
    keepalive = 60
    connect_packet = mosq_test.gen_connect("subpub-qos2-test-helper", keepalive=keepalive, proto_ver=5)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    mid = 1
    publish_packet = mosq_test.gen_publish("subpub/qos2", qos=2, mid=mid, payload="message1", proto_ver=5)
    pubrec_packet = mosq_test.gen_pubrec(mid, proto_ver=5)
    pubrel_packet = mosq_test.gen_pubrel(mid, proto_ver=5)
    pubcomp_packet = mosq_test.gen_pubcomp(mid, proto_ver=5)

    mid = 2
    publish_packet2 = mosq_test.gen_publish("subpub/qos2", qos=2, mid=mid, payload="message2", proto_ver=5)
    pubrec_packet2 = mosq_test.gen_pubrec(mid, proto_ver=5)
    pubrel_packet2 = mosq_test.gen_pubrel(mid, proto_ver=5)
    pubcomp_packet2 = mosq_test.gen_pubcomp(mid, proto_ver=5)

    mid = 3
    publish_packet3 = mosq_test.gen_publish("subpub/qos2", qos=2, mid=mid, payload="message3", proto_ver=5)
    pubrec_packet3 = mosq_test.gen_pubrec(mid, proto_ver=5)
    pubrel_packet3 = mosq_test.gen_pubrel(mid, proto_ver=5)
    pubcomp_packet3 = mosq_test.gen_pubcomp(mid, proto_ver=5)


    port = mosq_test.get_port()

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)

        mosq_test.do_send_receive(sock, publish_packet, pubrec_packet, "pubrec")
        mosq_test.do_send_receive(sock, pubrel_packet, pubcomp_packet, "pubcomp")

        mosq_test.do_send_receive(sock, publish_packet2, pubrec_packet2, "pubrec2")
        mosq_test.do_send_receive(sock, pubrel_packet2, pubcomp_packet2, "pubcomp2")

        mosq_test.do_send_receive(sock, publish_packet3, pubrec_packet3, "pubrec3")
        mosq_test.do_send_receive(sock, pubrel_packet3, pubcomp_packet3, "pubcomp3")
        rc = 0

        sock.close()
    except mosq_test.TestError:
        pass
    finally:
        if rc:
            print(stde.decode('utf-8'))
            print("proto_ver=%d" % (proto_ver))
            exit(rc)


do_test(proto_ver=5)
exit(rc)
