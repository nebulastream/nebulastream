#!/usr/bin/env python3

# Test whether shared subscriptions work

# Client 1 subscribes to #, non shared. Should receive everything.
# Client 2 subscribes to $share/one/share-test
# Client 3 subscribes to $share/one/share-test and $share/two/share-test
# Client 4 subscribes to $share/two/share-test
# Client 5 subscribes to $share/one/share-test

# A publish to "share-test" should always go to client 1.
# The first publish should also go to client 2 (share one) and client 3 (share two)
# The second publish should also go to client 3 (share one) and client 4 (share two)
# The third publish should also go to client 3 (share two) and client 5 (share one)

from mosq_test_helper import *

rc = 1
keepalive = 60
mid = 1

connect1_packet = mosq_test.gen_connect("client1", keepalive=keepalive, proto_ver=5)
connack1_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

connect2_packet = mosq_test.gen_connect("client2", keepalive=keepalive, proto_ver=5)
connack2_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

connect3_packet = mosq_test.gen_connect("client3", keepalive=keepalive, proto_ver=5)
connack3_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

connect4_packet = mosq_test.gen_connect("client4", keepalive=keepalive, proto_ver=5)
connack4_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

connect5_packet = mosq_test.gen_connect("client5", keepalive=keepalive, proto_ver=5)
connack5_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

subscribe1_packet = mosq_test.gen_subscribe(mid, "#", 0, proto_ver=5)
suback1_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

subscribe2_packet = mosq_test.gen_subscribe(mid, "$share/one/share-test", 0, proto_ver=5)
suback2_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

subscribe3a_packet = mosq_test.gen_subscribe(mid, "$share/one/share-test", 0, proto_ver=5)
suback3a_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

subscribe3b_packet = mosq_test.gen_subscribe(mid, "$share/two/share-test", 0, proto_ver=5)
suback3b_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

subscribe4_packet = mosq_test.gen_subscribe(mid, "$share/two/share-test", 0, proto_ver=5)
suback4_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

subscribe5_packet = mosq_test.gen_subscribe(mid, "$share/one/share-test", 0, proto_ver=5)
suback5_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

publish1_packet = mosq_test.gen_publish("share-test", qos=0, payload="message1", proto_ver=5)
publish2_packet = mosq_test.gen_publish("share-test", qos=0, payload="message2", proto_ver=5)
publish3_packet = mosq_test.gen_publish("share-test", qos=0, payload="message3", proto_ver=5)

mid = 2
unsubscribe1_packet = mosq_test.gen_unsubscribe(mid, "#", proto_ver=5)
unsuback1_packet = mosq_test.gen_unsuback(mid, proto_ver=5)

unsubscribe2_packet = mosq_test.gen_unsubscribe(mid, "$share/one/share-test", proto_ver=5)
unsuback2_packet = mosq_test.gen_unsuback(mid, proto_ver=5)

unsubscribe3a_packet = mosq_test.gen_unsubscribe(mid, "$share/one/share-test", proto_ver=5)
unsuback3a_packet = mosq_test.gen_unsuback(mid, proto_ver=5)

unsubscribe3b_packet = mosq_test.gen_unsubscribe(mid, "$share/two/share-test", proto_ver=5)
unsuback3b_packet = mosq_test.gen_unsuback(mid, proto_ver=5)

unsubscribe4_packet = mosq_test.gen_unsubscribe(mid, "$share/two/share-test", proto_ver=5)
unsuback4_packet = mosq_test.gen_unsuback(mid, proto_ver=5)

unsubscribe5_packet = mosq_test.gen_unsubscribe(mid, "$share/one/share-test", proto_ver=5)
unsuback5_packet = mosq_test.gen_unsuback(mid, proto_ver=5)


port = mosq_test.get_port()
broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

try:
    sock1 = mosq_test.do_client_connect(connect1_packet, connack1_packet, timeout=20, port=port)
    sock2 = mosq_test.do_client_connect(connect2_packet, connack2_packet, timeout=20, port=port)
    sock3 = mosq_test.do_client_connect(connect3_packet, connack3_packet, timeout=20, port=port)
    sock4 = mosq_test.do_client_connect(connect4_packet, connack4_packet, timeout=20, port=port)
    sock5 = mosq_test.do_client_connect(connect5_packet, connack5_packet, timeout=20, port=port)

    mosq_test.do_send_receive(sock1, subscribe1_packet, suback1_packet, "suback1")
    mosq_test.do_send_receive(sock2, subscribe2_packet, suback2_packet, "suback2")
    mosq_test.do_send_receive(sock3, subscribe3a_packet, suback3a_packet, "suback3a")
    mosq_test.do_send_receive(sock3, subscribe3b_packet, suback3b_packet, "suback3b")
    mosq_test.do_send_receive(sock4, subscribe4_packet, suback4_packet, "suback4")
    mosq_test.do_send_receive(sock5, subscribe5_packet, suback5_packet, "suback5")

    sock1.send(publish1_packet)
    mosq_test.expect_packet(sock1, "publish1 1", publish1_packet)
    mosq_test.expect_packet(sock2, "publish1 2", publish1_packet)
    mosq_test.expect_packet(sock3, "publish1 3", publish1_packet)

    sock1.send(publish2_packet)
    mosq_test.expect_packet(sock1, "publish2 1", publish2_packet)
    mosq_test.expect_packet(sock3, "publish2 3", publish2_packet)
    mosq_test.expect_packet(sock4, "publish2 4", publish2_packet)

    sock1.send(publish3_packet)
    mosq_test.expect_packet(sock1, "publish3 1", publish3_packet)
    mosq_test.expect_packet(sock3, "publish3 3", publish3_packet)
    mosq_test.expect_packet(sock5, "publish3 5", publish3_packet)
    mosq_test.do_send_receive(sock1, unsubscribe1_packet, unsuback1_packet, "unsuback1")
    mosq_test.do_send_receive(sock2, unsubscribe2_packet, unsuback2_packet, "unsuback2")
    mosq_test.do_send_receive(sock3, unsubscribe3a_packet, unsuback3a_packet, "unsuback3a")
    mosq_test.do_send_receive(sock3, unsubscribe3b_packet, unsuback3b_packet, "unsuback3b")
    mosq_test.do_send_receive(sock4, unsubscribe4_packet, unsuback4_packet, "unsuback4")
    mosq_test.do_send_receive(sock5, unsubscribe5_packet, unsuback5_packet, "unsuback5")

    rc = 0

    sock1.close()
    sock2.close()
    sock3.close()
    sock4.close()
    sock5.close()
except mosq_test.TestError:
    pass
finally:
    broker.terminate()
    broker.wait()
    (stdo, stde) = broker.communicate()
    if rc:
        print(stde.decode('utf-8'))

exit(rc)

