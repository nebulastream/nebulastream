#!/usr/bin/env python3

# Test whether a PUBLISH to $ topics QoS 1 results in the expected PUBACK packet.

from mosq_test_helper import *

mid = 1
def helper(topic, reason_code):
    global mid

    publish_packet = mosq_test.gen_publish(topic, qos=1, mid=mid, payload="message", proto_ver=5)
    if reason_code == 0:
        puback_packet = mosq_test.gen_puback(mid, proto_ver=5)
    else:
        puback_packet = mosq_test.gen_puback(mid, proto_ver=5, reason_code=reason_code)
    sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
    mosq_test.do_send_receive(sock, publish_packet, puback_packet, "puback%d"%(mid))


rc = 1
keepalive = 60
connect_packet = mosq_test.gen_connect("pub-test", keepalive=keepalive, proto_ver=5)
connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5)

port = mosq_test.get_port()
broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

try:
    sock = mosq_test.do_client_connect(connect_packet, connack_packet, port=port)
    helper("$SYS/broker/uptime", mqtt5_rc.MQTT_RC_NOT_AUTHORIZED)
    helper("$SYS/broker/connection/me", mqtt5_rc.MQTT_RC_NOT_AUTHORIZED)
    helper("$SYS/broker/connection/me/state", mqtt5_rc.MQTT_RC_NO_MATCHING_SUBSCRIBERS)
    helper("$share/share/topic", mqtt5_rc.MQTT_RC_NOT_AUTHORIZED)

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

