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

    KEEP_KEYS = [
        'AnatomicalLandmarkCoordinates', 'AcquisitionDuration', 'CogAtlasID',
        'CogPOID', 'CoilCombinationMethod', 'ConversionSoftware',
        'ConversionSoftwareVersion', 'DelayAfterTrigger', 'DelayTime',
        'DeviceSerialNumber', 'DwellTime', 'EchoNumbers', 'EchoTime',
        'EchoTrainLength', 'EffectiveEchoSpacing', 'FlipAngle',
        'GradientSetType', 'HighBit',
        'ImagedNucleus', 'ImageType', 'ImagingFrequency',
        'InPlanePhaseEncodingDirection', 'InstitutionName',
        'InstitutionAddress', 'InstitutionalDepartmentName',
        'Instructions', 'IntendedFor', 'InversionTime',
        'MRAcquisitionType', 'MagneticFieldStrength', 'Manufacturer',
        'ManufacturersModelName', 'MatrixCoilMode', 'Modality',
        'MRTransmitCoilSequence', 'MultibandAccelerationFactor',
        'NumberOfAverages', 'NumberOfPhaseEncodingSteps',
        'NumberOfVolumesDiscardedByScanner', 'NumberOfVolumesDiscardedByUser',
        'NumberShots', 'ParallelAcquisitionTechnique',
        'ParallelReductionFactorInPlane', 'PartialFourier',
        'PartialFourierDirection', 'PhaseEncodingDirection',
        'PixelBandwidth', 'ProtocolName', 'PulseSequenceDetails',
        'PulseSequenceType', 'ReceiveCoilActiveElements', 'ReceiveCoilName',
        'RepetitionTime', 'Rows',
        'SAR', 'ScanningSequence', 'ScanOptions', 'SequenceName',
        'SequenceVariant', 'SeriesDescription', 'SeriesNumber',
        'SliceEncodingDirection', 'SliceLocation',
        'SliceThickness', 'SliceTiming', 'SoftwareVersions',
        'SpacingBetweenSlices', 'StationName', 'TaskDescription',
        'TaskName', 'TotalReadoutTime', 'Units', 'VolumeTiming']

    for scan in scans:
        json_file = scan.filename.replace('.nii.gz', '.json')
        metadata = layout.get_metadata(scan.filename)
        metadata2 = {key: metadata[key] for key in KEEP_KEYS if key in
                     metadata.keys()}
        for key in KEEP_KEYS:
            if key not in metadata.keys() and 'global' in metadata.keys():
                if key in metadata['global']['const'].keys():
                    metadata2[key] = metadata['global']['const'][key]

        with open(json_file, 'w') as fo:
            json.dump(metadata2, fo, sort_keys=True, indent=4)


if __name__ == '__main__':
    folder = sys.argv[1]
    main(folder)
