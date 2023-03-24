#!/bin/sh

# Script to set up a local virtual environment and install required python libraries

echo "Welcome to the setup script. This will set up a local virtual environment and install required python libraries."
echo "You can rerun this script without any issues."
echo "================================================="

# Check if virtual environment directory exists
if [ -d "env" ]; then
    echo "Activating virtual environment..."
    . env/bin/activate
else
    echo "Virtual environment not found. Please run setup.sh first."
    exit 1
fi

# Set environment variable
export ENV=development

# Run main script
python3 main.py

# Deactivate virtual environment
deactivate
echo "Virtual environment deactivated."