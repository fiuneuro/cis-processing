################################################################################
# This script is designed to convert scans from the CIS to BIDS formatted datasets on the FIU
# HPC.

# Author: Michael Cody Riedel, miriedel@fiu.edu, Taylor Salo, tsalo006@fiu.edu 04/16/2018

################################################################################
################################################################################
##########################
# Get command line options
##########################
dicomdir=$1
outdir=$2
workdir=$3
heuristics=$4
project=$5
sub=$6
sess=$7
######################################
######################################

#############################################
# Begin by converting the data to BIDS format
#############################################

# Get date, gender, age, weight, height
tmp_dir1=($(ls $dicomdir/sub-$sub/ses-$sess/))

tar -C  $dicomdir/sub-$sub/ses-$sess/$tmp_dir1/ -cf \
    $dicomdir/sub-$sub/ses-$sess/sub-$sub-ses-$sess.tar scans/
rm -r $dicomdir/sub-$sub/ses-$sess/$tmp_dir1

# Create temporary directory in scratch folder for BIDS conversion
mkdir -p $workdir/$project-$sub-$sess

# Put data in BIDS format
heudiconv -d $dicomdir/sub-$sub/ses-$sess/sub-{subject}-ses-{session}.tar \
    -s $sub -ss $sess -f $heuristics -c dcm2niix \
    -o $workdir/$project-$sub-$sess/ --bids --overwrite

##############################################
# Check results, anonymize, and clean metadata
##############################################

if [ -d $workdir/$project-$sub-$sess/sub-$sub/ses-$sess ]; then
    chmod -R 774 $workdir/$project-$sub-$sess/sub-$sub/ses-$sess

    # Deface structural scans
    imglist=$(ls $workdir/$project-$sub-$sess/sub-$sub/ses-$sess/anat/*.nii.gz)
    for tmpimg in $imglist; do
        mri_deface $tmpimg /src/deface/talairach_mixed_with_skull.gca \
            /src/deface/face.gca $tmpimg
    done
    rm ./*.log

		# Add IntendedFor and TotalReadoutTime fields to jsons
    python complete_jsons.py -d $workdir/$project-$sub-$sess/ -s $sub \
        -ss $sess --overwrite

		# Remove extraneous fields from jsons
    python clean_metadata.py $workdir/$project-$sub-$sess/

    # Validate dataset and, if it passes, copy files to outdir
		bids-validator $workdir/$project-$sub-$sess/ > $workdir/$project-$sub-$sess.txt
    if grep -Fq "This dataset appears to be BIDS compatible." $workdir/$project-$sub-$sess.txt; then
        rm $workdir/$project-$sub-$sess.txt
        cp -r $workdir/$project-$sub-$sess/sub-$sub $outdir/

        filelist='CHANGES README participants.tsv dataset_description.json'
        for tmpfile in $filelist; do
            if [ -e $outdir/$tmpfile ]; then
                if [[ $tmpfile == "participants.tsv" ]]; then
								    pidinfo=$(sed -n '2p' $workdir/$project-$sub-$sess/$tmpfile)
										echo $pidinfo >> $outdir/$tmpfile
								else
									  if diff $workdir/$project-$sub-$sess/$tmpfile $outdir/$tmpfile > /dev/null; then
											  rm $workdir/$project-$sub-$sess/$tmpfile
					          else
					    	        cp $workdir/$project-$sub-$sess/$tmpfile \
                            $outdir/sub-$sub-ses-$sess-$tmpfile
					          fi
				        fi
			      else
				        mv $workdir/$project-$sub-$sess/$tmpfile $outdir/$tmpfile
			      fi
		    done
    else
    	  echo "Heudiconv-generated dataset failed BIDS validator."
    fi
else
	  echo "Heudiconv failed to convert this dataset to BIDS format."
fi
######################################
######################################
