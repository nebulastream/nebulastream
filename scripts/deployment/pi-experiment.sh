#!/bin/bash

# Function to run the experiment based on the experiment type
run_experiment() {
    local experiment_type=$1

    echo "Running experiment type: $experiment_type"

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

    echo "Sleeping for 30s..."
    sleep 30

    # Send query to coordinator
    echo "Sending query on coordinator..."
    ansible-playbook pi-avg-query.yml

    echo "Sleeping for 1 mins..."
    sleep 120

    # Stop everything
    echo "Stopping everything..."
    ansible-playbook stop-pi.yml

    # Sleep for 6 seconds
    echo "Sleeping for 6 seconds..."
    sleep 2

    echo "Experiment completed."
}

# Number of iterations
iterations=${1:-5}

echo "Deploying on pis..."
ansible-playbook deploy-pi.yml

echo "Starting Chameleon iterations..."
for (( i=1; i<=iterations; i++ ))
do
  run_experiment "chameleon"
done

echo "Starting fixed iterations..."
for (( i=1; i<=iterations; i++ ))
do
  run_experiment "fixed"
done

echo "All experiments completed."