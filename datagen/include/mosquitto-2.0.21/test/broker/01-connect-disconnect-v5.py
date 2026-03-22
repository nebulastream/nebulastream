#!/usr/bin/env python3

# loop through the different v5 DISCONNECT reason_code/properties options.

from mosq_test_helper import *

def disco_test(test, disconnect_packet):
    global rc

    sock1 = mosq_test.do_client_connect(connect1_packet, connack1_packet, port=port)
    mosq_test.do_send_receive(sock1, subscribe1_packet, suback1_packet, "suback1")


    sock2 = mosq_test.do_client_connect(connect2_packet, connack2_packet, port=port)
    sock2.send(disconnect_packet)
    sock2.close()

    # If this fails then we probably received the will
    mosq_test.do_ping(sock1)

    rc -= 1


rc = 4
keepalive = 10

connect1_packet = mosq_test.gen_connect("sub", proto_ver=5, keepalive=keepalive)
connack1_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

mid = 1
subscribe1_packet = mosq_test.gen_subscribe(mid, "#", 0, proto_ver=5)
suback1_packet = mosq_test.gen_suback(mid, 0, proto_ver=5)

connect2_packet = mosq_test.gen_connect("connect-disconnect-test", proto_ver=5, keepalive=keepalive, will_topic="failure", will_payload=b"failure")
connack2_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

port = mosq_test.get_port()
broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)


try:
    # No reason code, no properties, len=0
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5)
    disco_test("disco len=0", disconnect_packet)

    # Reason code, no properties, len=1
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=0)
    disco_test("disco len=1", disconnect_packet)

    # Reason code, empty properties, len=2
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=0, properties="")
    disco_test("disco len=2", disconnect_packet)

    # Reason code, one property, len>2
    props = mqtt5_props.gen_string_pair_prop(mqtt5_props.PROP_USER_PROPERTY, "key", "value")
    disconnect_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=0, properties=props)
    disco_test("disco len>2", disconnect_packet)
except mosq_test.TestError:
    pass
finally:
    broker.terminate()
    broker.wait()
    (stdo, stde) = broker.communicate()
    if rc:
        print(stde.decode('utf-8'))

if rc != 0:
    exit(rc)
