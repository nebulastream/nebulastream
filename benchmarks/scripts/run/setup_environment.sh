#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define the virtual environment directory
VENV_DIR="venv"

# Check if the virtual environment exists
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment already exists. Activating..."
else
    echo "Virtual environment not found. Creating a new one..."
    python3 -m venv $VENV_DIR
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source $VENV_DIR/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install required packages
echo "Installing required packages..."
pip install \
    pyyaml \
    pathlib \
    numpy \
    pandas \
    matplotlib \
    seaborn \
    tqdm

echo "Setup complete. To activate the virtual environment in the future, run:"
echo "source $VENV_DIR/bin/activate"
