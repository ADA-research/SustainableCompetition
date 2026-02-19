#!/bin/bash

#SBATCH --job-name=parsl-master
#SBATCH --output=parsl-master-%j.log
#SBATCH --error=parsl-master-%j.log
#SBATCH --signal=B:USR1@120             # 2 min warning before end
#SBATCH --time=10:00:00                 # total walltime
#SBATCH --partition=cook                # name of slurm partition to use for the master job
#SBATCH --nodes=1                       # only one node
#SBATCH --ntasks=1                      # single task (process)
#SBATCH --cpus-per-task=1               # single core

# Make sure to load the right modules from your environment
export PYTHONNOUSERSITE=1
export PYTHONUNBUFFERED=1
source ./venv/bin/activate

# Using 'exec' is important for signal handling to work correctly, allowing the process to receive signals directly rather than through a shell intermediary. This ensures that the shutdown handler in the code can catch signals like SIGUSR1 sent by SLURM before job termination, enabling graceful shutdown and cleanup.
exec python ./src/nemesis.py ./config/satcomp25micro2.yml --requeue "$SLURM_SUBMIT_DIR/nemesis.satcomp25micro2.slurm.sh"
