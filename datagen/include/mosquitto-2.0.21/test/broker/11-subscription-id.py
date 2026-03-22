#!/usr/bin/env python3

# Test whether a client message maintains its subscription id when persisted and restored.

from mosq_test_helper import *

def write_config(filename, port):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port))
        f.write("allow_anonymous true\n")
        f.write("persistence true\n")
        f.write("persistence_file mosquitto-%d.db\n" % (port))

port = mosq_test.get_port()
conf_file = os.path.basename(__file__).replace('.py', '.conf')
write_config(conf_file, port)

rc = 1
keepalive = 60

connect_packet = mosq_test.gen_connect(
    "persistent-subscription-test", keepalive=keepalive, clean_session=False, proto_ver=5, session_expiry=60
)
connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)
connack_packet2 = mosq_test.gen_connack(rc=0, flags=1, proto_ver=5)  # session present

mid = 1
props = mqtt5_props.gen_varint_prop(mqtt5_props.PROP_SUBSCRIPTION_IDENTIFIER, 53)
subscribe_packet = mosq_test.gen_subscribe(mid, "subpub/qos1", 1, proto_ver=5, properties=props)
suback_packet = mosq_test.gen_suback(mid, 1, proto_ver=5)

mid = 1
props = mqtt5_props.gen_varint_prop(mqtt5_props.PROP_SUBSCRIPTION_IDENTIFIER, 53)
publish_packet2 = mosq_test.gen_publish("subpub/qos1", qos=1, mid=mid, payload="message", proto_ver=5, properties=props)


helper_connect_packet = mosq_test.gen_connect("helper", keepalive=keepalive, clean_session=True, proto_ver=5)
helper_connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

mid = 1
helper_publish_packet = mosq_test.gen_publish("subpub/qos1", qos=1, mid=mid, payload="message", proto_ver=5)
helper_puback_packet = mosq_test.gen_puback(mid, proto_ver=5)


if os.path.exists('mosquitto-%d.db' % (port)):
    os.unlink('mosquitto-%d.db' % (port))

broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

(stdo1, stde1) = ("", "")
try:
    sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)
    mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback")
    sock.close()

    sock = mosq_test.do_client_connect(helper_connect_packet, helper_connack_packet, timeout=20, port=port)
    mosq_test.do_send_receive(sock, helper_publish_packet, helper_puback_packet, "helper puback")
    sock.close()

    broker.terminate()
    broker.wait()
    (stdo1, stde1) = broker.communicate()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet2, timeout=20, port=port)

    mosq_test.expect_packet(sock, "publish2", publish_packet2)
    rc = 0

    sock.close()
except mosq_test.TestError:
    pass
finally:
    os.remove(conf_file)
    broker.terminate()
    broker.wait()
    (stdo, stde) = broker.communicate()
    if rc:
        print(stde.decode('utf-8'))
    if os.path.exists('mosquitto-%d.db' % (port)):
        os.unlink('mosquitto-%d.db' % (port))
        pass


exit(rc)

