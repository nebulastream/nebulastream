from jinja2 import Template
import yaml
import os

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
# merge dictionaries
def merge_dicts(*args):
    merged_dict = {}
    for dictionary in args:
        merged_dict.update(dictionary)
    return merged_dict

# Render output and write to file
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


# Programm variables
config_file_path = 'benchmark.yml'

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
    'test': 'output_test_file.yml'
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


# Create output data

# coordinator 
coordinator_network_config = network_config.get('coordinator', {})
# print("coordinator_config: \n")
# print(coordinator_config)
# print("\ncoordinator_network_config: \n")
# print(coordinator_network_config)
# print("\nlogical_sources_config: \n")
# print(logical_sources_config)
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
i = 0
for i in range(0, len(workers_config)):
    print(f"i: {i}")
    print(f'worker_{i + 1}')
    print(workers_config.get(f'worker_{i + 1}', {}))
    worker_config = workers_config.get(f'worker_{i + 1}', {})

    # TODO: prepare coordinator and network values for worker
    #remove localWorkerIp and logLevel from worker_config
    # coordinator_config_worker = coordinator_config
    # # Check if the key exists and delete it
    # if 'localWorkerIp' in coordinator_config_worker:
    #     del coordinator_config_worker['localWorkerIp']
    # if 'logLevel' in coordinator_config_worker:
    #     del coordinator_config_worker['logLevel']
    # print(f"coordinator_config_worker: {coordinator_config_worker}")
    # merged_worker_config = merge_dicts(worker_config, coordinator_config)

    # TODO: logic to give individual worker just the physical sources it should have 
    rendering_context = {
        "worker": worker_config,
        "physicalSources": physical_sources_config
    }
    render_and_write_to_file(output_paths['worker'].replace('n', str(i+ 1)), rendering_context, templates_paths['worker'])


print('\nWorker whole data:')
print(rendering_context)


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



# other stuff to do
# create new image

# chekc if yaml is valid
test_output_yaml()