#!/usr/bin/env python3

# Test whether a persistent client that disconnects with DISCONNECT has its
# will published when it reconnects. It shouldn't. Bug 1273:
# https://github.com/eclipse/mosquitto/issues/1273

from mosq_test_helper import *


def do_test(proto_ver):
    rc = 1
    keepalive = 60

    connect1_packet = mosq_test.gen_connect("will-sub", keepalive=keepalive, proto_ver=proto_ver)
    connack1_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    mid = 1
    subscribe1_packet = mosq_test.gen_subscribe(mid, "will/test", 0, proto_ver=proto_ver)
    suback1_packet = mosq_test.gen_suback(mid, 0, proto_ver=proto_ver)

    connect2_packet = mosq_test.gen_connect("will-1273", keepalive=keepalive, will_topic="will/test", will_payload=b"will msg",clean_session=False, proto_ver=proto_ver, session_expiry=60)
    connack2a_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)
    connack2b_packet = mosq_test.gen_connack(rc=0, flags=1, proto_ver=proto_ver)

    disconnect_packet = mosq_test.gen_disconnect(proto_ver=proto_ver)

    publish_packet = mosq_test.gen_publish("will/test", qos=0, payload="alive", proto_ver=proto_ver)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        # Connect and subscribe will-sub
        sock1 = mosq_test.do_client_connect(connect1_packet, connack1_packet, timeout=30, port=port, connack_error="connack1")
        mosq_test.do_send_receive(sock1, subscribe1_packet, suback1_packet, "suback")

        # Connect will-1273
        sock2 = mosq_test.do_client_connect(connect2_packet, connack2a_packet, timeout=30, port=port)
        # Publish our "alive" message
        sock2.send(publish_packet)
        # Clean disconnect
        sock2.send(disconnect_packet)

        # will-1273 should get the "alive"
        mosq_test.expect_packet(sock1, "publish1", publish_packet)

        sock2.close()

        # Reconnect
        sock2 = mosq_test.do_client_connect(connect2_packet, connack2b_packet, timeout=30, port=port, connack_error="connack2")
        # will-1273 to publish "alive" again, and will-sub to receive it.
        sock2.send(publish_packet)
        mosq_test.expect_packet(sock1, "publish2", publish_packet)
        # Do a ping to make sure there are no other packets received.
        mosq_test.do_ping(sock1)
        rc = 0

        sock1.close()
        sock2.close()
    except mosq_test.TestError:
        pass
    finally:
        broker.terminate()
        broker.wait()
        (stdo, stde) = broker.communicate()
        if rc:
            print(stde.decode('utf-8'))
            exit(rc)

do_test(4)
do_test(5)
exit(0)
