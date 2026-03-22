#!/usr/bin/env python3

# Test whether a connection fail when using a auth_plugin that defer authentication.

from mosq_test_helper import *

def write_config(filename, port, plugin_ver):
    with open(filename, 'w') as f:
        f.write("port %d\n" % (port))
        f.write("auth_plugin c/auth_plugin_v%d.so\n" % (plugin_ver))
        f.write("allow_anonymous false\n")

def do_test(plugin_ver):
    port = mosq_test.get_port()
    conf_file = os.path.basename(__file__).replace('.py', '.conf')
    write_config(conf_file, port, plugin_ver)

    rc = 1
    keepalive = 10
    connect_packet = mosq_test.gen_connect("connect-uname-pwd-test", keepalive=keepalive, username="test-username@v2", password="doesNotMatter")
    connack_packet = mosq_test.gen_connack(rc=5)

    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    try:
        sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=20, port=port)
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
            exit(rc)

do_test(4)
do_test(5)
