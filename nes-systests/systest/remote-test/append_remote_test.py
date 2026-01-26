import argparse
import textwrap

parser = argparse.ArgumentParser(
    prog='Remote systest appending script',
    description='Appends a new systest in the systest_remote_test.bats file'
)

parser.add_argument('-p', '--remote_test_path', default='systest_remote_test.bats', help='Path to the bats file where the remote test should be appended to.')
parser.add_argument('-n', '--test_name', help='Name of the new systest')
parser.add_argument('-t', '--topology_path', help='Relative path from NES root to the topology yaml that should be used.')
parser.add_argument('-b', '--bandwidth', default='250mbit', help='Sets a maximum bandwidth for every docker container. Add as number with unit like 250mbit, 100kbit')
parser.add_argument('-c', '--systest_config', default="", help='The systest arguments as a whole string. Also includes worker arguments. Does not need the clusterConfig argument.')

args = parser.parse_args()

with open(args.remote_test_path, 'a') as f:
    test_string = textwrap.dedent(f"""
        @test "{args.test_name}" {{
            setup_distributed $NES_DIR/{args.topology_path} "{args.bandwidth}"
            run DOCKER_SYSTEST --clusterConfig {args.topology_path} --remote {args.systest_config}
            [ "$status" -eq 0 ]
        }}
    """)
    f.write(test_string)
