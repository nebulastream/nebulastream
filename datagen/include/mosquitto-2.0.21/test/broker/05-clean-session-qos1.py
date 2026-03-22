#!/usr/bin/env python3

# Test whether a clean session client has a QoS 1 message queued for it.

from mosq_test_helper import *

def helper(port):
    connect_packet = mosq_test.gen_connect("test-helper", keepalive=60)
    connack_packet = mosq_test.gen_connack(rc=0)

    mid = 128
    publish_packet = mosq_test.gen_publish("qos1/clean_session/test", qos=1, mid=mid, payload="clean-session-message")
    puback_packet = mosq_test.gen_puback(mid)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
    mosq_test.do_send_receive(sock, publish_packet, puback_packet, "puback")

    sock.close()


def do_test(proto_ver):
    rc = 1
    mid = 109
    keepalive = 60
    connect_packet = mosq_test.gen_connect("clean-qos2-test", keepalive=keepalive, clean_session=False, proto_ver=proto_ver, session_expiry=60)
    connack1_packet = mosq_test.gen_connack(flags=0, rc=0, proto_ver=proto_ver)
    connack2_packet = mosq_test.gen_connack(flags=1, rc=0, proto_ver=proto_ver)

    disconnect_packet = mosq_test.gen_disconnect(proto_ver=proto_ver)

    subscribe_packet = mosq_test.gen_subscribe(mid, "qos1/clean_session/test", 1, proto_ver=proto_ver)
    suback_packet = mosq_test.gen_suback(mid, 1, proto_ver=proto_ver)

    mid = 1
    publish_packet = mosq_test.gen_publish("qos1/clean_session/test", qos=1, mid=mid, payload="clean-session-message", proto_ver=proto_ver)
    puback_packet = mosq_test.gen_puback(mid, proto_ver=proto_ver)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack1_packet, port=port)
        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")

        sock.send(disconnect_packet)
        sock.close()

        helper(port)

        # Now reconnect and expect a publish message.
        sock = mosq_test.do_client_connect(connect_packet, connack2_packet, timeout=30, port=port)
        mosq_test.expect_packet(sock, "publish", publish_packet)
        sock.send(puback_packet)
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

