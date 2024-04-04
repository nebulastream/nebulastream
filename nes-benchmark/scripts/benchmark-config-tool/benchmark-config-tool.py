from jinja2 import Template
import yaml
import os

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
    directory = os.path.dirname(filepath)

    # Check if the directory exists before creating it
    if not os.path.exists(directory):
        os.makedirs(directory)  # This will create all directories in the path if they don't exist
        
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



#config_file_path = 'benchmark_nils_topo.yml'
config_file_path = 'kubernetes_config.yml'

templates_paths = {
    'coordinator': 'templates/coordinator.yml',
    'worker': 'templates/worker.yml',
    'source': 'templates/source.yml',
    'service': 'templates/kub_service.yaml',
    'kub_coordinator': 'templates/kub_coordinator.yaml',
    'kub_worker': 'templates/kub_worker.yaml',
    'kub_workerS': 'templates/kub_worker_s.yaml',
    'kub_configMap': 'templates/kub_configMap.yaml'
}
#TODO: write those file at the correct place for other scripts to read them 
output_paths = {
    'coordinator': 'output_files/coordinator.yml',
    'worker': 'output_files/workern.yml',
    'source': 'output_files/source.yml',
    'test': 'output_test_ file.yml',
    'service': 'output_files/kub_service.yaml',
    'kub_coordinator': 'output_files/kub_coordinator.yaml',
    'kub_worker': 'output_files/kub_worker.yaml',
    'kub_workerS': 'output_files/kub_worker_s.yaml',
    'kub_configMap': 'output_files/kub_configMap.yaml'
}

# TODO: refactor main config file 
# TODO: make it possible to vary the parameters given to the worker start command (create a list with all the parameters)
# - tempalte for the commadn paramenter themselves
# TODO: command vs config files

# Load and extract config data 
config_data = load_yaml_to_dict(config_file_path)

# Extract data for the different config files
network_config = config_data.get('network', {})
workers_config = config_data.get('workers', {})

print("\nNetwork config:")
print(network_config)

print("\nWorkers config:")
print(workers_config)

## Write kub config files
# service
render_and_write_to_file(output_paths['service'], network_config, templates_paths['service'])
# coordinator
render_and_write_to_file(output_paths['kub_coordinator'], network_config, templates_paths['kub_coordinator'])

# configMap
render_and_write_to_file(output_paths['kub_configMap'], network_config, templates_paths['kub_configMap'])


# worker
worker_config = merge_dicts(network_config, workers_config)
print("\nWorker config:")
print(worker_config)
render_and_write_to_file(output_paths['kub_worker'], worker_config, templates_paths['kub_worker'])

# workerS
workerS_config = merge_dicts(network_config, workers_config)
render_and_write_to_file(output_paths['kub_workerS'], workerS_config, templates_paths['kub_workerS'])

test_output_yaml()