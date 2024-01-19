#!/bin/bash

# Default experiment type
experiment_type=${1:-fixed}

echo "Deploying on pis..."
ansible-playbook deploy-pi.yml

# Run the corresponding Ansible playbook
if [ "$experiment_type" == "fixed" ]; then
    echo "Running 'fixed' experiment..."
    ansible-playbook start-pi.yml
elif [ "$experiment_type" == "chameleon" ]; then
    echo "Running 'chameleon' experiment..."
    ansible-playbook start-pi-chameleon.yml
else
    echo "Unknown experiment type: $experiment_type"
    exit 1
fi

# Sleep for a minute
echo "Sleeping for 60 seconds..."
sleep 60

# Send query to coordinator
echo "Sending query on coordinator..."
ansible-playbook pi-avg-query.yml
sleep 300

# Send query to coordinator
echo "Stopping everything..."
ansible-playbook stop-pi.yml

echo "Experiment completed."
