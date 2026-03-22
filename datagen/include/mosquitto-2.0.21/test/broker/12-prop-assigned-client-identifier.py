#!/usr/bin/env python3

# Test whether sending a non zero session expiry interval in DISCONNECT after
# having sent a zero session expiry interval is treated correctly in MQTT v5.

from mosq_test_helper import *




def do_test(clean_start):
    rc = 1
    keepalive = 10
    connect_packet = mosq_test.gen_connect(None, proto_ver=5, keepalive=keepalive, clean_session=clean_start)

    props = mqtt5_props.gen_string_prop(mqtt5_props.PROP_ASSIGNED_CLIENT_IDENTIFIER, "auto-00000000-0000-0000-0000-000000000000")
    connack_packet = mosq_test.gen_connack(rc=0, proto_ver=5, properties=props)

    props = mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_SESSION_EXPIRY_INTERVAL, 1)
    disconnect_client_packet = mosq_test.gen_disconnect(proto_ver=5, properties=props)

    disconnect_server_packet = mosq_test.gen_disconnect(proto_ver=5, reason_code=130)

    port = mosq_test.get_port()
    broker = mosq_test.start_broker(filename=os.path.basename(__file__), port=port)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect(("localhost", port))

        sock.send(connect_packet)
        connack_recvd = sock.recv(len(connack_packet))

        if connack_recvd[0:12] == connack_packet[0:12]:
            # FIXME - this test could be tightened up a lot
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


do_test(True)
do_test(False)
exit(0)

