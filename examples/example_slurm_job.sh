#!/bin/bash
#---Number of cores
#SBATCH -c 12

#---Job's name in SLURM system
#SBATCH -J bids

#---Error file
#SBATCH -e bids_err

#---Output file
#SBATCH -o bids_out

#---Queue name
#SBATCH --account [lab_queue]

#---Partition
#SBATCH -p centos7
########################################################
export NPROCS=`echo $LSB_HOSTS | wc -w`
export OMP_NUM_THREADS=$NPROCS
. $MODULESHOME/../global/profile.modules
module load singularity-3

python cis_proc.py -d [/path/to/tarfile] -w [/scratch/path/to/working/directory/] \
  -b [/path/to/bids/dataset/] --config [/path/to/config/file] --sub [SUBJECTID] \
  --ses [SESSION] --n_procs $NPROCS
