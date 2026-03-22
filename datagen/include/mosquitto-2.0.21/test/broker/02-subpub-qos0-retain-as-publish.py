#!/usr/bin/env python3

# Test whether a client subscribed to a topic with retain-as-published set works as expected.
# MQTT v5

from mosq_test_helper import *

def do_test():
    rc = 1
    keepalive = 60
    connect_packet = mosq_test.gen_connect("subpub-qos1-test", keepalive=keepalive, proto_ver=5)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    mid = 530
    subscribe1_packet = mosq_test.gen_subscribe(mid, "subpub/normal", 0, proto_ver=5)
    suback1_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

    mid = 531
    subscribe2_packet = mosq_test.gen_subscribe(mid, "subpub/rap", 0 | mqtt5_opts.MQTT_SUB_OPT_RETAIN_AS_PUBLISHED, proto_ver=5)
    suback2_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

    publish1_packet = mosq_test.gen_publish("subpub/normal", qos=0, retain=True, payload="message", proto_ver=5)
    publish2_packet = mosq_test.gen_publish("subpub/rap", qos=0, retain=True, payload="message", proto_ver=5)

    publish1r_packet = mosq_test.gen_publish("subpub/normal", qos=0, retain=False, payload="message", proto_ver=5)
    publish2r_packet = mosq_test.gen_publish("subpub/rap", qos=0, retain=True, payload="message", proto_ver=5)

    mid = 1
    publish3_packet = mosq_test.gen_publish("subpub/receive", qos=1, mid=mid, payload="success", proto_ver=5)


    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)

        mosq_test.do_send_receive(sock, subscribe1_packet, suback1_packet, "suback1")
        mosq_test.do_send_receive(sock, subscribe2_packet, suback2_packet, "suback2")

        mosq_test.do_send_receive(sock, publish1_packet, publish1r_packet, "publish1")
        mosq_test.do_send_receive(sock, publish2_packet, publish2r_packet, "publish2")
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
