import yaml
import os
import argparse
import traceback
from pathlib import Path


def gen_clion_xml(name: str, program: str, args: list[str], output_dir: Path):
    xml = f"""
<component name="ProjectRunConfigurationManager">
  <configuration
    name="{name}"
    TARGET_NAME="{program}"
    PROGRAM_PARAMS="{" ".join(args)}"
    RUN_TARGET_PROJECT_NAME="NES"
    RUN_TARGET_NAME="{program}"
    CONFIG_NAME="Debug"
    default="false"
    ELEVATE="false"
    EMULATE_TERMINAL="false"
    factoryName="Application"
    PASS_PARENT_ENVS_2="true"
    PROJECT_NAME="NES"
    REDIRECT_INPUT="false"
    {'REDIRECT_INPUT_PATH="$FilePath$"' if "$FilePath$" in args else ""}
    type="CMakeRunConfiguration"
    USE_EXTERNAL_CONSOLE="false"
  >
    <method v="2">
      <option name="com.jetbrains.cidr.execution.CidrBuildBeforeRunTaskProvider$BuildBeforeRunTask" enabled="true" />
    </method>
  </configuration>
</component>
    """

    filename = f"{name}.xml"
    with open(output_dir / filename, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"Generated: {output_dir / filename}")


def generate_all_nodes_xml(topology_name, all_nodes, output_dir):
    """
    Generates an XML configuration file that includes all nodes.
    """
    # Create PROGRAM_PARAMS string
    program_params = "\n".join(
        [
            f' <toRun name="nes-single-node-worker-{topology_name}-{node["grpc"]}-{node["connection"]}" type="CMakeRunConfiguration" />'
            for node in all_nodes
        ]
    )

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
    with open(output_path, "w") as f:
        f.write(xml_template)
    print(f"Generated: {output_path}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate XML configuration files from YAML input"
    )
    parser.add_argument(
        "-i", "--input", required=True, help="Path to the input YAML file"
    )
    parser.add_argument(
        "-o", "--output", default=".", help="Output directory for XML files"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_file = args.input
    output_dir = Path(args.output)

    try:
        # Read YAML file
        with open(input_file, "r") as f:
            yaml_data = yaml.safe_load(f)

        topology_name = input_file.split("/")[-1].split(".")[0]
        print(f"Generating {topology_name}.xml")
        # Extract nodes
        nodes = yaml_data.get("nodes", [])
        ports = []

        for node in nodes:
            connection_port = node.get("connection", "").split(":")[
                1
            ]  # Assuming format like '127.0.0.1:9091'
            grpc_port = node.get("grpc", "").split(":")[1]

            node_data = {
                "connection": connection_port,
                "grpc": grpc_port,
                "cpus": node.get("cpus", 4),
                "buffers": node.get("buffers", 1024),
            }
            ports.append(node_data)

        # Generate XML files for each node
        for node_data in ports:
            # Extract connection and grpc ports
            name = f"nes-single-node-worker-{topology_name}-{node_data['grpc']}-{node_data['connection']}"
            args = [
                f"--worker.numberOfBuffersInGlobalBufferManager={node_data['buffers']}",
                f"--grpc=127.0.0.1:{node_data['grpc']}",
                f"--data=127.0.0.1:{node_data['connection']}",
                "--worker.queryCompiler.nautilusBackend=COMPILER",
                f"--worker.queryEngine.numberOfWorkerThreads={node_data['cpus']}",
            ]
            gen_clion_xml(name, "nes-single-node-worker", args, output_dir)

        # Generate XML file for all nodes
        generate_all_nodes_xml(topology_name, ports, output_dir)
        topo_file = os.path.abspath(input_file)
        gen_clion_xml(
            "Nebuli Start Query",
            "nes-nebuli",
            ["-t", topo_file, "register", "-x", "-i", "$FilePath$"],
            output_dir,
        )
        gen_clion_xml(
            "Nebuli Dump Query",
            "nes-nebuli",
            ["-t", topo_file, "dump", "-i", "$FilePath$", "-o", "/tmp"],
            output_dir,
        )

    except FileNotFoundError:
        traceback.print_exc()
        print(f"Error: Input file '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
