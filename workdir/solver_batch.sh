#!/bin/bash -x
#SBATCH --job-name=solver_batch
#SBATCH --output=solver_batch.out
#SBATCH --error=solver_batch.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64         
#SBATCH --mem=0                    # Use all memory on the node
#SBATCH --exclusive                

# Define arrays for cores and memory per instance
cores_array=(4 8 16 32)              # Cores for each run
mem_array=(32 64 128 256)            # Memory for each run

# Get the number of physical CPUs (sockets)
num_sockets=$(lscpu | grep "Socket(s)" | awk '{print $2}')

# Launch one instance per socket
for current_cores in "${cores_array[@]}"; do
    for current_mem in "${mem_array[@]}"; do
        # Get environment hash
        env_hash=$(python ../src/utils/environment.py $current_cores $current_mem)
        for run in $(seq 0 5); do
            for socket in $(seq 0 1); do
                # Bind to the specific socket and run the script in the background
                srun --cpus-per-task=$current_cores \
                    --mem=$current_mem \
                    --cpu-bind=sockets \
                    bash ./run_solver.sh $env_hash $1 ./00d1fe07ab948b348bb3fb423b1ef40d-lec_mult_KvW_12x11.sanitized.cnf $current_mem 500 > "output_${env_hash}_${socket}_${run}.log" 2>&1 &
            done
            wait
        done
    done
done
