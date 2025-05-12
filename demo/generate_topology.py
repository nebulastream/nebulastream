#!/usr/bin/env python3
import random
import yaml
import argparse
import os
import ipaddress
import itertools
import networkx as nx
from typing import List, Dict, Any

def parse_args():
    parser = argparse.ArgumentParser(description='Generate a random topology config')
    parser.add_argument('--sources-yaml', required=True, help='Path to the sources YAML file')
    parser.add_argument('--num-nodes', type=int, default=5, help='Number of nodes in the topology')
    parser.add_argument('--base-port', type=int, default=8080, help='Base port for connections')
    parser.add_argument('--base-grpc-port', type=int, default=9090, help='Base port for gRPC')
    parser.add_argument('--output', default='topology_config.yaml', help='Output file path')
    parser.add_argument('--testdata-path', default='TESTDATA', help='Path to replace TESTDATA in file paths')
    parser.add_argument('--min-sources-per-node', type=int, default=1, help='Minimum sources per leaf node')
    parser.add_argument('--max-sources-per-node', type=int, default=3, help='Maximum sources per leaf node')
    return parser.parse_args()

def generate_tree(num_nodes: int) -> nx.DiGraph:
    """Generate a random tree with specified number of nodes."""
    # Create a tree with num_nodes
    G = nx.DiGraph()

    if num_nodes <= 0:
        return G

    # Add root node
    G.add_node(0)

    # Add remaining nodes
    for i in range(1, num_nodes):
        # Select a random existing node as parent
        parent = random.randint(0, i-1)
        G.add_edge(parent, i)  # parent -> child

    return G

def generate_ip_config(base_ip: str, base_port: int, base_grpc_port: int, num_nodes: int) -> Dict[int, Dict]:
    """Generate IP configurations for nodes."""
    ip_configs = {}

    # Parse base IP
    ip_obj = ipaddress.IPv4Address(base_ip)

    for i in range(num_nodes):
        node_ip = str(ip_obj)
        ip_configs[i] = {
            'connection': f'{node_ip}:{base_port + i}',
            'grpc': f'{node_ip}:{base_grpc_port + i}'
        }

    return ip_configs

def assign_sources_to_leaves(G: nx.DiGraph, sources: List[Dict],
                             min_sources: int, max_sources: int) -> Dict[int, List[Dict]]:
    """Assign sources to leaf nodes."""
    leaves = [node for node in G.nodes() if G.out_degree(node) == 0]
    if not leaves:
        # If no leaves, make all nodes potential source hosts
        leaves = list(G.nodes())

    node_sources = {}

    # Make a copy of sources for random selection
    available_sources = sources.copy()
    random.shuffle(available_sources)

    for leaf in leaves:
        # Determine how many sources to assign to this leaf
        num_sources = min(random.randint(min_sources, max_sources), len(available_sources))

        if num_sources > 0:
            node_sources[leaf] = []
            for _ in range(num_sources):
                if available_sources:
                    source = available_sources.pop(0)
                    node_sources[leaf].append(source)

        # If we've used all sources, stop assigning
        if not available_sources:
            break

    # If there are still unassigned sources, distribute them among the leaves
    if available_sources:
        leaf_cycle = itertools.cycle(leaves)
        for source in available_sources:
            leaf = next(leaf_cycle)
            if leaf not in node_sources:
                node_sources[leaf] = []
            node_sources[leaf].append(source)

    return node_sources

def generate_config(args):
    # Load sources from YAML
    with open(args.sources_yaml, 'r') as f:
        sources_data = yaml.safe_load(f)

    sources = sources_data.get('sources', [])

    # Generate a random tree topology
    G = generate_tree(args.num_nodes)

    # Generate IP configurations
    ip_configs = generate_ip_config('127.0.0.1', args.base_port, args.base_grpc_port, args.num_nodes)

    # Assign sources to leaf nodes
    node_sources = assign_sources_to_leaves(
        G, sources, args.min_sources_per_node, args.max_sources_per_node
    )

    # Create config structure
    config = {
        'logical': [],
        'nodes': []
    }

    # Add logical sources
    for source in sources:
        logical_source = {
            'name': source['name'],
            'schema': source['schema']
        }
        config['logical'].append(logical_source)

    # Create nodes
    for node_id in range(args.num_nodes):
        node_config = {
            'connection': ip_configs[node_id]['connection'],
            'grpc': ip_configs[node_id]['grpc'],
            'capacity': random.choice([0, 100]),
            'cpus': random.randint(1, 4),
            'buffers': random.randint(100000, 200000)
        }

        # Add links
        children = [child for _, child in G.out_edges(node_id)]
        parents = [parent for parent, _ in G.in_edges(node_id)]

        if children or parents:
            node_config['links'] = {}

            if children:
                node_config['links']['downstreams'] = [ip_configs[child]['connection'] for child in children]

            if parents:
                node_config['links']['upstreams'] = [ip_configs[parent]['connection'] for parent in parents]

        # Add sources if this node has any
        if node_id in node_sources:
            node_config['physical'] = []

            for source in node_sources[node_id]:
                physical_source = {
                    'logical': source['name'],
                    'parserConfig': {
                        'type': 'CSV'
                    }
                }

                # Configure the source
                if 'path' in source:
                    path = source['path']

                    # Replace TESTDATA if needed
                    if path.startswith(args.testdata_path):
                        path = path  # Keep as is
                    elif os.path.exists(path):
                        # It's a local file
                        physical_source['sourceConfig'] = {
                            'type': 'File',
                            'filePath': path
                        }
                    else:
                        # Assume it's a TCP source
                        physical_source['sourceConfig'] = {
                            'type': 'TCP',
                            'socketHost': '127.0.0.1',
                            'socketPort': random.randint(6000, 7000)
                        }
                else:
                    # Default to TCP source if no path
                    physical_source['sourceConfig'] = {
                        'type': 'TCP',
                        'socketHost': '127.0.0.1',
                        'socketPort': random.randint(6000, 7000)
                    }

                # Add path if it exists
                if 'path' in source and os.path.exists(source['path']):
                    physical_source['sourceConfig'] = {
                        'type': 'File',
                        'filePath': source['path']
                    }

                node_config['physical'].append(physical_source)

        # Add some sinks to random nodes (30% chance)
        if random.random() < 0.3:
            node_config['sinks'] = [
                {
                    'name': f"sink_{random.randint(1, 1000)}",
                    'type': 'Print',
                    'config': {
                        'ingestion': random.choice([100, 1000]),
                        'inputFormat': 'CSV'
                    }
                }
            ]

        config['nodes'].append(node_config)

    # Add at least one sink to the root node
    root_node = config['nodes'][0]
    if 'sinks' not in root_node:
        root_node['sinks'] = []

    root_node['sinks'].append({
        'name': 'fast_csv_sink',
        'type': 'Print',
        'config': {
            'ingestion': 100,
            'inputFormat': 'CSV'
        }
    })

    return config

def main():
    args = parse_args()

    # Generate the configuration
    config = generate_config(args)

    # Save to file
    with open(args.output, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    print(f"Generated topology with {args.num_nodes} nodes saved to {args.output}")

if __name__ == '__main__':
    main()
