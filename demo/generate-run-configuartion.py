#!/usr/bin/env python3

import yaml
import os
import argparse
import traceback

def generate_single_node_xml(topology_name, node_data, output_dir):
    """
    Generates an XML configuration file for a single node.
    """
    # XML template for a single node
    xml_template = f"""
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="nes-single-node-worker-{topology_name}-{node_data['grpc']}-{node_data['connection']}" type="CMakeRunConfiguration" factoryName="Application" focusToolWindowBeforeRun="true" PROGRAM_PARAMS="--worker.numberOfBuffersInGlobalBufferManager={node_data['buffers']} --grpc=127.0.0.1:{node_data['grpc']} --data=127.0.0.1:{node_data['connection']} --worker.queryCompiler.nautilusBackend=COMPILER --worker.queryEngine.numberOfWorkerThreads={node_data['cpus']}" REDIRECT_INPUT="false" ELEVATE="false" USE_EXTERNAL_CONSOLE="false" EMULATE_TERMINAL="false" PASS_PARENT_ENVS_2="true" PROJECT_NAME="NES" TARGET_NAME="nes-single-node-worker" CONFIG_NAME="Debug" RUN_TARGET_PROJECT_NAME="NES" RUN_TARGET_NAME="nes-single-node-worker">
    <method v="2">
      <option name="com.jetbrains.cidr.execution.CidrBuildBeforeRunTaskProvider$BuildBeforeRunTask" enabled="true" />
    </method>
  </configuration>
</component>
    """
    # Generate output filename
    filename = f"nes-single-node-worker-{topology_name}-{node_data['grpc']}.xml"
    output_path = os.path.join(output_dir, filename)

    # Write XML to file
    with open(output_path, 'w') as f:
        f.write(xml_template)
    print(f"Generated: {output_path}")


def generate_nebuli_dump(topology_name, topology_file, output_dir):
    nebuli_start = f"""
    <component name="ProjectRunConfigurationManager">
      <configuration default="false" name="nebuli dump {topology_name}" type="CMakeRunConfiguration" factoryName="Application" PROGRAM_PARAMS="-t {topology_file} dump -i $FilePath$ -o /tmp" REDIRECT_INPUT="false" REDIRECT_INPUT_PATH="$FilePath$" ELEVATE="false" USE_EXTERNAL_CONSOLE="false" EMULATE_TERMINAL="false" PASS_PARENT_ENVS_2="true" PROJECT_NAME="NES" TARGET_NAME="nes-nebuli" CONFIG_NAME="Debug" RUN_TARGET_PROJECT_NAME="NES" RUN_TARGET_NAME="nes-nebuli">
        <method v="2">
          <option name="com.jetbrains.cidr.execution.CidrBuildBeforeRunTaskProvider$BuildBeforeRunTask" enabled="true" />
        </method>
      </configuration>
    </component>
    """

    filename = f"Run_nebuli_dump_{topology_name}.xml"
    output_path = os.path.join(output_dir, filename)

    # Write XML to file
    with open(output_path, 'w') as f:
        f.write(nebuli_start)
    print(f"Generated: {output_path}")

def generate_nebuli_start(topology_name, topology_file, output_dir):
    nebuli_start = f"""
    <component name="ProjectRunConfigurationManager">
      <configuration default="false" name="nebuli start {topology_name}" type="CMakeRunConfiguration" factoryName="Application" PROGRAM_PARAMS="-t {topology_file} register -x -i $FilePath$" REDIRECT_INPUT="false" REDIRECT_INPUT_PATH="$FilePath$" ELEVATE="false" USE_EXTERNAL_CONSOLE="false" EMULATE_TERMINAL="false" PASS_PARENT_ENVS_2="true" PROJECT_NAME="NES" TARGET_NAME="nes-nebuli" CONFIG_NAME="Debug" RUN_TARGET_PROJECT_NAME="NES" RUN_TARGET_NAME="nes-nebuli">
        <method v="2">
          <option name="com.jetbrains.cidr.execution.CidrBuildBeforeRunTaskProvider$BuildBeforeRunTask" enabled="true" />
        </method>
      </configuration>
    </component>
    """

    filename = f"Run_nebuli_{topology_name}.xml"
    output_path = os.path.join(output_dir, filename)

    # Write XML to file
    with open(output_path, 'w') as f:
        f.write(nebuli_start)
    print(f"Generated: {output_path}")


def generate_all_nodes_xml(topology_name, all_nodes, output_dir):
    """
    Generates an XML configuration file that includes all nodes.
    """
    # Create PROGRAM_PARAMS string
    program_params = "\n".join(
        [
            f' <toRun name="nes-single-node-worker-{topology_name}-{node['grpc']}-{node['connection']}" type="CMakeRunConfiguration" />'
            for node in all_nodes])

    # XML template for all nodes
    xml_template = f"""
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="start_{topology_name}" type="CompoundRunConfigurationType">
{program_params}
    <method v="2" />
  </configuration>
</component>"""
    # Generate output filename
    filename = f"Run_{topology_name}.xml"
    output_path = os.path.join(output_dir, filename)

    # Write XML to file
    with open(output_path, 'w') as f:
        f.write(xml_template)
    print(f"Generated: {output_path}")


def parse_args():
    parser = argparse.ArgumentParser(description='Generate XML configuration files from YAML input')
    parser.add_argument('-i', '--input', required=True, help='Path to the input YAML file')
    parser.add_argument('-o', '--output', default='.', help='Output directory for XML files')
    return parser.parse_args()


def main():
    args = parse_args()
    input_file = args.input
    output_dir = args.output

    try:
        # Read YAML file
        with open(input_file, 'r') as f:
            yaml_data = yaml.safe_load(f)

        topology_name = input_file.split('/')[-1].split(".")[0]
        print(f"Generating {topology_name}.xml")
        # Extract nodes
        nodes = yaml_data.get('nodes', [])
        ports = []

        for node in nodes:
            connection_port = node.get('connection', '').split(':')[1]  # Assuming format like '127.0.0.1:9091'
            grpc_port = node.get('grpc', '').split(':')[1]

            node_data = {
                'connection': connection_port,
                'grpc': grpc_port,
                'cpus': node.get('cpus', 4),
                'buffers': node.get('buffers', 1024)
            }
            ports.append(node_data)

        # Generate XML files for each node
        for node_data in ports:
            # Extract connection and grpc ports

            generate_single_node_xml(topology_name, node_data, output_dir)

        # Generate XML file for all nodes
        generate_all_nodes_xml(topology_name, ports, output_dir)
        generate_nebuli_start(topology_name, os.path.abspath(input_file), output_dir)
        generate_nebuli_dump(topology_name, os.path.abspath(input_file), output_dir)

    except FileNotFoundError:
        traceback.print_exc()
        print(f"Error: Input file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
