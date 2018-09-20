#!/bin/bash
#---Number of cores
#BSUB -n 12
#BSUB -R "span[ptile=12]"

#---Job's name in LSF system
#BSUB -J bids

#---Error file
#BSUB -eo bids_err

#---Output file
#BSUB -oo bids_out

#---LSF Queue name
#BSUB -q PQ_normal

##########################################################
# Set up environmental variables.
##########################################################
export NPROCS=`echo $LSB_HOSTS | wc -w`
export OMP_NUM_THREADS=$NPROCS

. $MODULESHOME/../global/profile.modules
module load singularity

##########################################################
##########################################################

python cis_proc.py -d [/path/to/tarfile] -w [/scratch/path/to/working/directory/] \
  -b [/path/to/bids/dataset/] --config [/path/to/config/file] --sub [SUBJECTID] \
  --ses [SESSION] --n_procs $NPROCS
