#!/bin/bash
#SBATCH --job-name=solver_batch
#SBATCH --output=solver_batch%A.out
#SBATCH --error=solver_batch%A.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32         # Total cores on the node (adjust as needed)
#SBATCH --mem=0                    # Use all memory on the node
#SBATCH --exclusive                # Reserve the full node

# Define arrays for cores and memory per instance
cores_array=(4 8 16 32)              # Cores for each instance
mem_array=(32G 64G 128G 256G)            # Memory for each instance

# Get the number of physical CPUs (sockets)
num_sockets=$(lscpu | grep "Socket(s)" | awk '{print $2}')

# Launch one instance per socket
for socket in $(seq 0 1); do
    current_cores=${cores_array[$socket]}
    current_mem=${mem_array[$socket]}
    # Get environment hash
    env_hash=$(./update_environment.sh $current_cores 64 $current_mem)

    # Bind to the specific socket and run the script in the background
    numactl --cpunodebind=$socket --membind=$socket \
        taskset -c $((socket * 32))-$((socket * 32 + current_cores - 1)) \
        ./run_solver.sh > "output_${env_hash}_${socket}.log" 2>&1 &
done

# Wait for all background processes to finish
wait
