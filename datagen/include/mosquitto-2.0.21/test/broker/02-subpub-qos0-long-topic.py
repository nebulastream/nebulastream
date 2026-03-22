#!/usr/bin/env python3

# Test whether a client subscribed to a topic receives its own message sent to that topic, for long topics.

from mosq_test_helper import *

def do_test(topic, succeeds):
    rc = 1
    mid = 53
    keepalive = 60
    connect_packet = mosq_test.gen_connect("subpub-qos0-test", keepalive=keepalive)
    connack_packet = mosq_test.gen_connack(rc=0)

    subscribe_packet = mosq_test.gen_subscribe(mid, topic, 0)
    suback_packet = mosq_test.gen_suback(mid, 0)

    publish_packet = mosq_test.gen_publish(topic, qos=0, payload="message")

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)

        if succeeds == True:
            mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")
            mosq_test.do_send_receive(sock, publish_packet, publish_packet, "publish")
        else:
            mosq_test.do_send_receive(sock, subscribe_packet, b"", "suback")

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

do_test("/"*200, True) # 200 max hierarchy limit
do_test("abc/"*199+"d", True) # 200 max hierarchy limit, longer overall string than 200

do_test("/"*201, False) # Exceeds 200 max hierarchy limit
do_test("abc/"*201+"d", False) # Exceeds 200 max hierarchy limit, longer overall string than 200


exit(0)

