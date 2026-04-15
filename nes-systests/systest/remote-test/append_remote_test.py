import argparse
import os
import re
import textwrap

import yaml

parser = argparse.ArgumentParser(
    prog='Remote systest appending script',
    description='Appends a new systest in the systest_remote_test.bats file'
)

parser.add_argument('-p', '--remote_test_path', default='systest_remote_test.bats', help='Path to the bats file where the remote test should be appended to.')
parser.add_argument('-n', '--test_name', help='Name of the new systest')
parser.add_argument('-t', '--topology_path', help='Relative path from NES root to the topology yaml that should be used.')
parser.add_argument('-b', '--bandwidth', default='250mbit', help='Sets a maximum bandwidth for every docker container. Add as number with unit like 250mbit, 100kbit')
parser.add_argument('-r', '--nes_root', default=os.environ.get('NES_DIR', '.'), help='Absolute path to the NES root directory. Used to locate the topology yaml on disk so it can be copied and rewritten.')
parser.add_argument('-c', '--systest_config', default="", help='The systest arguments as a whole string. Also includes worker arguments. Does not need the clusterConfig argument.')

args = parser.parse_args()


def bandwidth_to_mbit(bandwidth: str) -> int:
    match = re.fullmatch(r'\s*(\d+)\s*(kbit|mbit|gbit)\s*', bandwidth, re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not parse bandwidth '{bandwidth}'. Expected e.g. 250mbit, 100kbit, 1gbit.")
    value, unit = int(match.group(1)), match.group(2).lower()
    if unit == 'kbit':
        return max(1, value // 1000)
    if unit == 'mbit':
        return value
    return value * 1000


bandwidth_mb = bandwidth_to_mbit(args.bandwidth)

source_topology_abs = os.path.join(args.nes_root, args.topology_path)
with open(source_topology_abs, 'r') as f:
    topology = yaml.safe_load(f)

for worker in topology.get('workers', []) or []:
    config = worker.setdefault('config', {})
    worker_cfg = config.setdefault('worker', {})
    network_cfg = worker_cfg.setdefault('network', {})
    network_cfg['max_network_bandwidth_mb'] = bandwidth_mb

base, ext = os.path.splitext(args.topology_path)
new_topology_rel = f"{base}_{args.test_name}{ext}"
new_topology_abs = os.path.join(args.nes_root, new_topology_rel)
with open(new_topology_abs, 'w') as f:
    yaml.safe_dump(topology, f, sort_keys=False)

with open(args.remote_test_path, 'a') as f:
    test_string = textwrap.dedent(f"""
        @test "{args.test_name}" {{
            setup_distributed $NES_DIR/{new_topology_rel} "{args.bandwidth}"
            run DOCKER_SYSTEST --clusterConfig $NES_DIR/{new_topology_rel} --remote {args.systest_config}
            [ "$status" -eq 0 ]
        }}
    """)
    f.write(test_string)
