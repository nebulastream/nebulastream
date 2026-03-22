#!/usr/bin/env python3

# Check whether the broker handles a v5 PUBREL with all combinations
# of with/without reason code and properties.

from mosq_test_helper import *

def len_test(test, pubrel_packet):
    rc = 1
    mid = 3265
    keepalive = 60
    connect_packet = mosq_test.gen_connect("pub-test", keepalive=keepalive, clean_session=False, proto_ver=5)
    connack_packet = mosq_test.gen_connack(flags=0, rc=0, proto_ver=5)

    mid = 1
    publish_packet = mosq_test.gen_publish("qos2/len/test", qos=2, mid=mid, payload="len-message", proto_ver=5)
    pubrec_packet = mosq_test.gen_pubrec(mid)
    pubcomp_packet = mosq_test.gen_pubcomp(mid)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)

        mosq_test.do_send_receive(sock, publish_packet, pubrec_packet, "pubrec")
        mosq_test.do_send_receive(sock, pubrel_packet, pubcomp_packet, "pubcomp")

        mosq_test.do_ping(sock)
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

    if rc != 0:
        print(test)
        exit(rc)


# No reason code, no properties
pubrel_packet = mosq_test.gen_pubrel(1)
len_test("qos2 len 2", pubrel_packet)

# Reason code, no properties
pubrel_packet = mosq_test.gen_pubrel(1, proto_ver=5, reason_code=0x00)
len_test("qos2 len 3", pubrel_packet)

# Reason code, empty properties
pubrel_packet = mosq_test.gen_pubrel(1, proto_ver=5, reason_code=0x00, properties="")
len_test("qos2 len 4", pubrel_packet)

# Reason code, one property
props = mqtt5_props.gen_string_pair_prop(mqtt5_props.PROP_USER_PROPERTY, "key", "value")
pubrel_packet = mosq_test.gen_pubrel(1, proto_ver=5, reason_code=0x00, properties=props)
len_test("qos2 len >5", pubrel_packet)
