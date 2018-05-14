"""
Author: Taylor Salo, tsalo006@fiu.edu
Edited: Michael Riedel, miriedel@fiu.edu; 4/18/2018
"""
from __future__ import print_function

import sys
import json

from bids.grabbids import BIDSLayout


def main(bids_dir):
    layout = BIDSLayout(bids_dir)
    scans = layout.get(extensions='nii.gz')

    KEEP_KEYS = ['ConversionSoftware', 'ConversionSoftwareVersion',
                 'EchoTime', 'EffectiveEchoSpacing', 'FlipAngle',
                 'MagneticFieldStrength', 'Manufacturer',
                 'ManufacturersModelName', 'PhaseEncodingDirection',
                 'ProtocolName', 'RepetitionTime', 'ScanningSequence',
                 'SequenceName', 'SequenceVariant', 'SeriesDescription',
                 'SliceTiming', 'TaskName', 'FlipAngle', 'TotalReadoutTime',
                 'EchoNumbers', 'EchoTrainLength', 'HighBit',
                 'NumberOfAverages', 'Modality', 'ImagedNucleus',
                 'ImagingFrequency', 'InPlanePhaseEncodingDirection',
                 'MRAcquisitionType', 'NumberOfPhaseEncodingSteps',
                 'PixelBandwidth', 'Rows', 'SAR', 'SliceLocation',
                 'SliceThickness', 'SpacingBetweenSlices', 'IntendedFor',
                 'MultibandAccelerationFactor']

    for scan in scans:
        json_file = scan.filename.replace('.nii.gz', '.json')
        metadata = layout.get_metadata(scan.filename)
        metadata2 = {key: metadata[key] for key in KEEP_KEYS if key in
                     metadata.keys()}
        for key in KEEP_KEYS:
            if key not in metadata.keys() and \
               key in metadata['global']['const'].keys():
                metadata2[key] = metadata['global']['const'][key]

        with open(json_file, 'w') as fo:
            json.dump(metadata2, fo, sort_keys=True, indent=4)


if __name__ == '__main__':
    folder = sys.argv[1]
    main(folder)
