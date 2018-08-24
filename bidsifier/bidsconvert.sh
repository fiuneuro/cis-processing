################################################################################
# This script is designed to convert scans from the CIS to BIDS formatted datasets on the FIU
# HPC.

# Author: Michael Cody Riedel, miriedel@fiu.edu, Taylor Salo, tsalo006@fiu.edu 04/16/2018

################################################################################
################################################################################
##########################
# Get command line options
##########################
scratchdir=$1
heuristics=$2
project=$3
sub=$4
sess=${5:-None}

######################################
######################################

#############################################
# Begin by converting the data to BIDS format
#############################################
if [ "$sess" = "None" ]; then
  # Put data in BIDS format
  heudiconv -d $scratchdir/sub-{subject}.tar -s $sub -f \
    $heuristics -c dcm2niix -o $scratchdir/bids/ --bids --overwrite
  minipath=sub-$sub
else
  # Put data in BIDS format
  heudiconv -d $scratchdir/sub-{subject}-ses-{session}.tar -s $sub -ss $sess -f \
    $heuristics -c dcm2niix -o $scratchdir/bids/ --bids --overwrite
  minipath=sub-$sub/ses-$sess
fi

##############################################
# Check results, anonymize, and clean metadata
##############################################
if [ -d $scratchdir/bids/$minipath ]; then
  chmod -R 774 $scratchdir/bids/$minipath

  # Deface structural scans
  imglist=$(ls $scratchdir/bids/$minipath/anat/*.nii.gz)
  for tmpimg in $imglist; do
    mri_deface $tmpimg /src/deface/talairach_mixed_with_skull.gca \
      /src/deface/face.gca $tmpimg
  done
  rm ./*.log

  # Add IntendedFor and TotalReadoutTime fields to jsons
  python /scripts/complete_jsons.py -d $scratchdir/bids/ -s $sub -ss $sess --overwrite

  # Remove extraneous fields from jsons
  python /scripts/clean_metadata.py $scratchdir/bids/

  # Validate dataset and, if it passes, copy files to outdir
  bids-validator $scratchdir/bids/ --ignoreWarnings > $scratchdir/validator.txt
else
  echo "FAILED" > $scratchdir/validator.txt
  echo "Heudiconv failed to convert this dataset to BIDS format."
fi
######################################
######################################
