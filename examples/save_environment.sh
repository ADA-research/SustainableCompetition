#!/bin/bash

# Define the CSV file
CSV_FILE="run_environment.csv"

# Collect environment features
nb_assigned_cores=$1
nb_available_core=$2
assigned_memory=$3
machine_memory=$(free -m | awk '/Mem:/ {print $2}')
memory_type="DDR4"  # Adjust or detect dynamically if possible
OS=$(grep -oP '(?<=^NAME=").+' /etc/os-release | tr -d '"')
kernel_version=$(uname -r)
activated_modules=$(module list 2>&1 | grep -v "No modules" | tr '\n' ';')
cpu_freq=$(lscpu | grep "CPU MHz" | awk '{print $3}')
cpu_model=$(lscpu | grep "Model name" | cut -d':' -f2 | xargs)
cpu_cache_size=$(lscpu | grep "L3 cache" | awk '{print $3}')

# Generate a hash for the current environment
env_hash=$(echo "$nb_assigned_cores,$nb_available_core,$assigned_memory,$machine_memory,$memory_type,$OS,$kernel_version,$activated_modules,$cpu_freq,$cpu_model,$cpu_cache_size" | md5sum | awk '{print $1}')

# Check if the hash exists in the CSV
if ! grep -q "^$env_hash," "$CSV_FILE"; then
    # Append the new environment to the CSV
    echo "$env_hash,$nb_assigned_cores,$nb_available_core,$assigned_memory,$machine_memory,$memory_type,$OS,$kernel_version,$activated_modules,$cpu_freq,$cpu_model,$cpu_cache_size" >> "$CSV_FILE"
fi

# Output the hash for use in the Slurm script
echo "$env_hash"
