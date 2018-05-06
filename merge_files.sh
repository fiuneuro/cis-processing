# Merge files from scratch into real BIDS directory

##########################
# Get command line options
##########################
scratchdir=$1
outdir=$2
project=$3
sub=$4
sess=$5
######################################

if grep -Fq "This dataset appears to be BIDS compatible." $scratchdir/validator.txt; then
    rm $scratchdir/validator.txt

    cp -r $scratchdir/bids/sub-$sub $outdir/

    filelist='CHANGES README participants.tsv dataset_description.json'
    for tmpfile in $filelist; do
        if [ -e $outdir/$tmpfile ]; then
            if [[ $tmpfile == "participants.tsv" ]]; then
                pidinfo=$(sed -n '2p' $scratchdir/$project-$sub-$sess/$tmpfile)
                echo $pidinfo >> $outdir/$tmpfile
            else
                if diff $scratchdir/$project-$sub-$sess/$tmpfile $outdir/$tmpfile > /dev/null; then
                    rm $scratchdir/$project-$sub-$sess/$tmpfile
                else
                    cp $scratchdir/$project-$sub-$sess/$tmpfile \
                        $outdir/sub-$sub-ses-$sess-$tmpfile
                fi
            fi
        else
            mv $scratchdir/$project-$sub-$sess/$tmpfile $outdir/$tmpfile
        fi
    done
else
    echo "Heudiconv-generated dataset failed BIDS validator."
fi
