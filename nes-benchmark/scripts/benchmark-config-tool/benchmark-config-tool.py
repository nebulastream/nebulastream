from jinja2 import Template
import yaml
import os
import subprocess
import time
import atexit
import sys
import requests
import json
import copy

# TODO: For now just creates config files working with the tutorial.
# Package installation: pip install Jinja2

# Function to load a YAML file to a dictionary
def load_yaml_to_dict(filepath):
    with open(filepath, 'r') as file:
        data = yaml.safe_load(file)
    return data


# Function to load a template from a file
def load_template(filepath):
    with open(filepath, 'r') as file:
        return file.read()

# TODO: maybe not merge but coordinator. worker. in templates 
def merge_dicts(*args):
    merged_dict = {}
    for dictionary in args:
        merged_dict.update(dictionary)
    return merged_dict

def render_and_write_to_file(filepath, content, template):

    # Load tempaltes
    template_content = load_template(template)
    coordinator_template = Template(template_content)

    # Render the template with the configuration data
    rendered_template = coordinator_template.render(content)

    # Write the rendered template to a new YAML file
    with open(filepath, 'w') as file:
        file.write(rendered_template)

def test_output_yaml():
    # Loop through each file in the directory
    print("\n ------Testing output files------\n")
    folder_path = "output_files/"
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        # Check if the current path is a file and not a directory
        if os.path.isfile(file_path):
            try:
                load_yaml_to_dict(file_path)
                print(f"\n{file_path} is valid\n")
            except Exception as e:
                print(f"\n{file_path} is not valid: {e}\n")

# Function to check if the REST API is available   
def check_rest_api(base_url):
    conn_check_url = f"{base_url}/v1/nes/connectivity/check"
    try:
        response = requests.get(conn_check_url)

        # Check if the server responded with a 200 OK
        if response.status_code == 200:
            print("The REST Server is available and operational.")
            return True

        else:
            print("The server responded, but there might be an issue. Response code:", response.status_code)
            sys.exit()

    except requests.ConnectionError:
        print("Failed to connect to the REST Server. Please check the URL and your network connection.")
        sys.exit()


def run_query(base_url, query, placement):
    
    query_url = f"{base_url}/v1/nes/query/execute-query"

    headers = {
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    }

    payload = {
        "userQuery": f"{query}",
        "placement": f"{placement}",
        # Uncomment the following lines if you need to include these optional fields
        # "faultTolerance": "[FAULT TOLERANCE TYPE]",
        # "lineage": "[LINEAGE MODE]",
    }
    print(f"payload: {payload}")


    try:
        response = requests.post(query_url, headers=headers, data=json.dumps(payload))

        # Check if the server responded with a 200 OK
        # Check for successful response
        if response.status_code == 202:
            print("Request was successful.")
            print("Response:", response.text)
        else:
            print("Query Request failed with status code:", response.status_code)
            print("Response:", response.text)
    except requests.ConnectionError:
        print("Failed to connect to the REST Server. Please check the URL and your network connection.")


# after quiting clean up all sub processes 
def cleanup():
    # Kill all worker subprocesses
    for proc in worker_processes:
        proc.kill()

    coordinator_process.kill()

# register cleanup function
atexit.register(cleanup)



# Programm variables
config_file_path = 'benchmark_nils_topo.yml'

templates_paths = {
    'coordinator': 'templates/coordinator.yml',
    'worker': 'templates/worker.yml',
    'source': 'templates/source.yml',
    'docker-compose': 'templates/docker-compose.yml',
}
#TODO: write those file at the correct place for nes to read them 
output_paths = {
    'coordinator': 'output_files/coordinator.yml',
    'worker': 'output_files/workern.yml',
    'source': 'output_files/source.yml',
    'docker-compose': 'output_files/docker-compose.yml',
    'test': 'output_test_ file.yml'
}


# Load and extract config data 

# Load the configuration file
config_data = load_yaml_to_dict(config_file_path)

# Extract data for the different config files
network_config = config_data.get('network', {})
coordinator_config = config_data.get('coordinator', {})
workers_config = config_data.get('workers', {})
logical_sources_config = config_data.get('logicalSources', {})
physical_sources_config = config_data.get('physicalSources', {})
network_sources = network_config.get('sources', {})
worker_phy_sources_conns = network_sources.get('connections', {})


# Create output data

# coordinator 
coordinator_network_config = network_config.get('coordinator', {})
merged_coordinator_config = merge_dicts(coordinator_config, coordinator_network_config)
rendering_context = {
    "coordinator": merged_coordinator_config,
    "logicalSources": logical_sources_config  # Keep the list structure intact
}

print("\nCoordinator whole data: \n")
print(rendering_context)
render_and_write_to_file(output_paths['coordinator'], rendering_context, templates_paths['coordinator'])


# worker
worker_config = None
worker_base_config = None
worker_configs = []
i = 0
for i in range(0, len(workers_config)):
    print(f"i: {i}")
    print(f'worker_{i + 1}')
    print(workers_config.get(f'worker_{i + 1}', {}))
    worker_config = workers_config.get(f'worker_{i + 1}', {})
    if i == 0:
        worker_base_config = worker_config

    # worker_config_element = {
    #     "id": i,
    #     "data": worker_config
    # }
    worker_configs.append(worker_config)

    rendering_context = {
        "worker": worker_config,
        "physicalSources": physical_sources_config
    }
    render_and_write_to_file(output_paths['worker'].replace('n', str(i+ 1)), rendering_context, templates_paths['worker'])

print("\nWorker dict:\n")
print(worker_configs)
print("\nWorker elements:\n")
for worker in worker_configs:
    #print(worker)
    print("\n")
    print("worker: \n")
    for key, value in worker.items():
        print(f"{key}: {value}")
# print('\ni: ', i)
number_worker = i + 1
# print(f"number_worker: {number_worker}")

# print('\nWorker whole data:')
# print(rendering_context)

print("\nWorker sources connections:")
print(worker_phy_sources_conns)

# worker matching logic:
rendering_context_workers = []
new_worker_configs = []
for worker in worker_configs:
    worker_tmp = copy.deepcopy(worker_base_config)
    for key, value in worker.items():
            print("\n")
            print(f"key: {key}, value: {value}")
            print(f"worker:{worker}")
            worker_tmp[key] = value
            print(f"worker_tmp: {worker_tmp}")
    new_worker_configs.append(worker_tmp)

            
print("\nWorker dict2:\n")
print(new_worker_configs)
# print("\nWorker elements:\n")
# for worker in new_worker_configs:
#     #print(worker)
#     print("\n")
#     print("worker: \n")
#     for key, value in worker.items():
#         print(f"{key}: {value}")

# add sources and create rendering context
print("\nPhysical sources:")
print(physical_sources_config)
physical_sources_config_list = []
for source in physical_sources_config:
    print(f"source: {source}")
    #tmp_souirce = {physical_source_name}

i = 0
for worker in new_worker_configs:
    print(f"worker: {i}")
    curent_worker_sources_list = []
    for conn in worker_phy_sources_conns:
        print(f"conn: {conn}")
        for key, value in conn.items():
            worker_id = None
            if key == 'worker_id':
                print(f"worker_id: {value}")
                worker_id = value.split('_')[1]
                worker_id = int(worker_id)
                worker_id = worker_id - 1
                if worker_id == i:
                    print(worker_id)
                    source_key = conn["physical_source_name"]
                    for source in physical_sources_config:
                        if source_key == source["physicalSourceName"]:
                            curent_worker_sources_list.append(source)
                    #new_worker_configs[worker_id]['sources'] = conn["physical_source_name"]
                    #new_worker_configs[worker_id]['sources'] = conn['sources']
                            
    # create rendering context
    rendering_context = {
        "worker": worker,
        "physicalSources": curent_worker_sources_list }
    render_and_write_to_file(output_paths['worker'].replace('n', str(i+ 1)), rendering_context, templates_paths['worker'])
    i = i + 1

print("\nWorker sources:")
print(curent_worker_sources_list)

for conn in worker_phy_sources_conns:
    sources_for_worker = {}
    print(f"conn: {conn}")
    for key, value in conn.items():
        worker_id = None
        if key == 'worker_id':
            print(f"worker_id: {value}")
            worker_id = value.split('_')[1]
            worker_id = int(worker_id)
            worker_id = worker_id - 1
            print(worker_id)
        


# docker-compose
# TODO: create entry points with configPaths (make it in general more complex)

rendering_context = {
    "coordinator_image": network_config.get('coordinator', {}).get('image', {}),
    "worker_image": network_config.get('worker', {}).get('image', {})  
}

print('\nDocker-compose whole data:')
print(rendering_context)
# Render output and write to file

render_and_write_to_file(output_paths['docker-compose'], network_config, templates_paths['docker-compose'])


# chekc if yaml is valid
test_output_yaml()



# Test query run on nils topo example 

coordinator_cmd = './nesCoordinator'
# coordinator_cmd_with_args = './nesCoordinator --configPath=/home/tim/Documents/work/nebulastream/config/coordinator.yaml'
coordinator_args = ["--configPath=", "/home/tim/Documents/work/nebulastream/config/coordinator.yaml"]
coordinator_arg = "--configPath=/home/tim/Documents/work/nebulastream/config/coordinator.yaml"
#coordinator_arg = "--configPath=/home/tim/Documents/work/nebulastream/nes-benchmark/scripts/benchmark-config-tool/output_files/coordinator.yml"

worker_cmd = "./nesWorker"
worker_args = "--configPath=/home/tim/Documents/work/nebulastream/config/worker.yaml"


coordinator_dir_path = "build/nes-coordinator"
worker_dir_path = "build/nes-worker"

print(os.getcwd()
)

path = os.getcwd()
split_at = "nebulastream"

# Splitting the path manually
parts = path.split(os.sep)
split_index = parts.index(split_at) + 1
before_specific, after_specific = os.sep.join(parts[:split_index]), os.sep.join(parts[split_index:])
nes_path = before_specific
print("Before:", before_specific)
print("After:", after_specific)


# create coordinator arguments
#coordinator_arg = f"--configPath={os.path.join(nes_path, 'config/coordinator.yaml')}"
coordinator_arg = f"--configPath={os.path.join(nes_path, 'nes-benchmark/scripts/benchmark-config-tool/output_files/coordinator.yml')}"
print(f"coordinator_arg: {coordinator_arg}")




worker_args = []
#number_worker = 0
# create worker arguments
for i in range(1, number_worker+1):
    worker_arg = f"--configPath={os.path.join(nes_path, f'nes-benchmark/scripts/benchmark-config-tool/output_files/worker{i}.yml')}"
    print(f"worker_arg: {worker_arg}")
    worker_args.append(worker_arg)


# create coordinator command with args
coordinator_path = os.path.join(nes_path, coordinator_dir_path)
coordinator_cmd_with_args = f"{coordinator_cmd} {coordinator_arg}"
print(f"nes_path: {nes_path}")
print(f"Coordinator path: {coordinator_path}")
print(os.path.exists(coordinator_path))


# create worker command with args
worker_path = os.path.join(nes_path, worker_dir_path)
worker_cmds_with_args = []
for wrorker_arg in worker_args:
    worker_cmds_with_args.append(f"{worker_cmd} {wrorker_arg}")

for test in worker_cmds_with_args:
    print(f"test: {test}")


# start coordinator and workers
outputs = []
coordinator_process = subprocess.Popen([coordinator_cmd, coordinator_arg], cwd=coordinator_path)
# coordinator_process.wait() #TODO dass entfernen da es gerade solange wartet bis cooordinator wieder kaputt geht um worker zu starten 
print("Coordinator started...")
time.sleep(5)

worker_processes = []

# worker_process = subprocess.Popen([worker_cmd, worker_args[0]], cwd=worker_path)

for arg in worker_args:
    print(f"{arg} started ")
    worker_process = subprocess.Popen([worker_cmd, arg], cwd=worker_path)
    worker_processes.append(worker_process)
    time.sleep(2)


print("All nodes started...")


while True:

    #TODO: make it dynamic (read from config file)
    base_url = 'http://127.0.0.1:8081'
    check_rest_api(base_url)

    query = 'Query::from("cars").filter(Attribute("speed") > 50.0000).sink(NullOutputSinkDescriptor::create());'
    placement = "BottomUp"

    run_query(base_url, query=query, placement=placement)

    #TODO: add a way to quit the programm while its running without the need to sue enter 
    input = input()
    if input == "q":
        print("Quitting ...")
        sys.exit()
    else:
        print("Type q to quit.")
