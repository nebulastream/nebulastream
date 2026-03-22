#!/usr/bin/env python3

# Test whether a retained PUBLISH is cleared when a zero length retained
# message is published to a topic.

from mosq_test_helper import *


def do_test(proto_ver):
    rc = 1
    keepalive = 60
    connect_packet = mosq_test.gen_connect("retain-clear-test", keepalive=keepalive, proto_ver=proto_ver)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=proto_ver)

    publish_packet = mosq_test.gen_publish("retain/clear/test", qos=0, payload="retained message", retain=True, proto_ver=proto_ver)
    retain_clear_packet = mosq_test.gen_publish("retain/clear/test", qos=0, payload=None, retain=True, proto_ver=proto_ver)
    mid_sub = 592
    subscribe_packet = mosq_test.gen_subscribe(mid_sub, "retain/clear/test", 0, proto_ver=proto_ver)
    suback_packet = mosq_test.gen_suback(mid_sub, 0, proto_ver=proto_ver)

    mid_unsub = 593
    unsubscribe_packet = mosq_test.gen_unsubscribe(mid_unsub, "retain/clear/test", proto_ver=proto_ver)
    unsuback_packet = mosq_test.gen_unsuback(mid_unsub, proto_ver=proto_ver)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=4, port=port)
        # Send retained message
        sock.send(publish_packet)
        # Subscribe to topic, we should get the retained message back.
        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")

        mosq_test.expect_packet(sock, "publish", publish_packet)
        # Now unsubscribe from the topic before we clear the retained
        # message.
        mosq_test.do_send_receive(sock, unsubscribe_packet, unsuback_packet, "unsuback")

        # Now clear the retained message.
        sock.send(retain_clear_packet)

        # Subscribe to topic, we shouldn't get anything back apart
        # from the SUBACK.
        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")
        time.sleep(1)
        # If we do get something back, it should be before this ping, so if
        # this succeeds then we're ok.
        mosq_test.do_ping(sock)
        # This is the expected event
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

