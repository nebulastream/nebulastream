#!/usr/bin/env python3

# Does the broker respect receive maximum==1?
# MQTT v5

from mosq_test_helper import *

def do_test():
    rc = 1
    keepalive = 60
    props = mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_RECEIVE_MAXIMUM, 1)
    connect_packet = mosq_test.gen_connect("subpub-qos2-test", keepalive=keepalive, proto_ver=5, properties=props)
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

    mid = 1
    subscribe_packet = mosq_test.gen_subscribe(mid, "subpub/qos2", 2, proto_ver=5)
    suback_packet = mosq_test.gen_suback(mid, 2, proto_ver=5)

    mid = 1
    publish_packet1 = mosq_test.gen_publish("subpub/qos2", qos=2, mid=mid, payload="message1", proto_ver=5)
    pubrec_packet1 = mosq_test.gen_pubrec(mid, proto_ver=5)
    pubrel_packet1 = mosq_test.gen_pubrel(mid, proto_ver=5)
    pubcomp_packet1 = mosq_test.gen_pubcomp(mid, proto_ver=5)

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
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)

        mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")

        pub = subprocess.Popen(['./02-subpub-qos2-receive-maximum-helper.py', str(port)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pub.wait()
        (stdo, stde) = pub.communicate()

        mosq_test.expect_packet(sock, "publish1", publish_packet1)
        mosq_test.do_send_receive(sock, pubrec_packet1, pubrel_packet1, "pubrel1")
        sock.send(pubcomp_packet1)

        mosq_test.expect_packet(sock, "publish2", publish_packet2)
        mosq_test.do_send_receive(sock, pubrec_packet2, pubrel_packet2, "pubrel2")
        sock.send(pubcomp_packet2)

        mosq_test.expect_packet(sock, "publish3", publish_packet3)
        mosq_test.do_send_receive(sock, pubrec_packet3, pubrel_packet3, "pubrel3")
        sock.send(pubcomp_packet3)

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
