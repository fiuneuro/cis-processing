#!/usr/bin/env python
"""
Fill in the gaps left by heudiconv in the json files for a BIDS dataset.
For example:
-   Assign IntendedFor field to each field map, based on acquisition time
    relative to functional scans.
-   Add PhaseEncodingDirection and TotalReadoutTime fields to field map jsons.
-   Add TaskName to functional scan jsons.
"""
import json
import bisect
import argparse
import os.path as op

import nibabel as nib
from dateutil.parser import parse
from bids.grabbids import BIDSLayout


def files_to_dict(file_list):
    """Convert list of BIDS Files to dictionary where key is
    acquisition time (datetime.datetime object) and value is
    the File object.
    """
    out_dict = {}
    for file_ in file_list:
        fname = file_.filename
        with open(fname, 'r') as f_obj:
            data = json.load(f_obj)
        acq_time = int(data['SeriesNumber'])
        out_dict[acq_time] = file_
    return out_dict


def complete_fmap_jsons(bids_dir, subs, ses, overwrite):
    """
    bids_dir: path to BIDS dataset
    subs: list of subjects
    ses: string of session
    overwrite: bool

    Assign 'IntendedFor' field to field maps in BIDS dataset.
    Uses the most recent field map before each functional or DWI scan, based on
    acquisition time.
    """
    layout = BIDSLayout(bids_dir)
    data_suffix = '.nii.gz'

    for sid in subs:
        # Remove potential trailing slash with op.abspath
        if not sid.startswith('sub-'):
            temp_sid = 'sub-{0}'.format(sid)
        else:
            temp_sid = sid
        subj_dir = op.abspath(op.join(bids_dir, temp_sid))

        for dir_ in ['AP', 'PA']:
            for acq in ['func', 'dwi']:
                # Get json files for field maps
                if ses:
                    fmap_jsons = layout.get(subject=sid, session=ses,
                                            modality='fmap', extensions='json',
                                            dir=dir_, acq=acq)
                else:
                    fmap_jsons = layout.get(subject=sid,
                                            modality='fmap', extensions='json',
                                            dir=dir_, acq=acq)

                if fmap_jsons:
                    fmap_dict = files_to_dict(fmap_jsons)
                    dts = sorted(fmap_dict.keys())
                    intendedfor_dict = {fmap.filename: [] for fmap in
                                        fmap_jsons}

                    # Get all scans with associated field maps
                    if ses:
                        dat_jsons = layout.get(subject=sid, session=ses,
                                               modality=acq, extensions='json')
                    else:
                        dat_jsons = layout.get(subject=sid,
                                               modality=acq, extensions='json')

                    dat_jsons = files_to_dict(dat_jsons)
                    for dat_file in dat_jsons.keys():
                        fn, _ = op.splitext(dat_jsons[dat_file].filename)
                        fn += data_suffix
                        fn = fn.split(subj_dir)[-1][1:]  # Get relative path

                        # Find most immediate field map before scan
                        idx = bisect.bisect_right(dts, dat_file) - 1

                        # if there is no field map *before* the scan, grab the
                        # first field map
                        if idx == -1:
                            idx = 0
                        fmap_file = fmap_dict[dts[idx]].filename
                        intendedfor_dict[fmap_file].append(fn)

                    for fmap_file in intendedfor_dict.keys():
                        with open(fmap_file, 'r') as f_obj:
                            data = json.load(f_obj)

                        if overwrite or ('IntendedFor' not in data.keys()):
                            data['IntendedFor'] = intendedfor_dict[fmap_file]
                            with open(fmap_file, 'w') as f_obj:
                                json.dump(data, f_obj, sort_keys=True,
                                          indent=4)

        niftis = layout.get(subject=sid, session=ses, modality='fmap',
                            extensions='nii.gz')
        for nifti in niftis:
            nifti_fname = nifti.filename
            img = nib.load(nifti_fname)

            # get_nearest doesn't work with field maps atm
            data = layout.get_metadata(fn)
            json_fname = nifti_fname.replace('.nii.gz', '.json')

            if overwrite or 'TotalReadoutTime' not in data.keys():
                # This next bit taken shamelessly from fmriprep
                acc = float(data.get('ParallelReductionFactorInPlane', 1.0))
                pe_idx = {'i': 0,
                          'j': 1,
                          'k': 2}[data['PhaseEncodingDirection'][0]]
                npe = img.shape[pe_idx]
                etl = npe // acc
                ees = data.get('EffectiveEchoSpacing', None)
                if ees is None:
                    raise Exception('Field "EffectiveEchoSpacing" not '
                                    'found in json')
                trt = ees * (etl - 1)
                data['TotalReadoutTime'] = trt
                with open(json_fname, 'w') as f_obj:
                    json.dump(data, f_obj, sort_keys=True, indent=4)


def complete_func_jsons(bids_dir, subs, ses, overwrite):
    """
    bids_dir: path to BIDS dataset
    subs: list of subjects
    ses: string of session
    overwrite: bool

    Calculate TotalReadoutTime and add TaskName
    """
    layout = BIDSLayout(bids_dir)
    for sid in subs:
        # Assign TaskName
        for task in layout.get_tasks():
            if ses:
                niftis = layout.get(subject=sid, session=ses, modality='func',
                                    task=task, extensions='nii.gz')
            else:
                niftis = layout.get(subject=sid, modality='func',
                                    task=task, extensions='nii.gz')

            for nifti in niftis:
                nifti_fname = nifti.filename
                img = nib.load(nifti_fname)
                # get_nearest doesn't work with field maps atm
                data = layout.get_metadata(nifti_fname)
                json_fname = nifti_fname.replace('.nii.gz', '.json')

                if overwrite or 'TotalReadoutTime' not in data.keys():
                    # This next bit taken shamelessly from fmriprep
                    acc = float(data.get('ParallelReductionFactorInPlane',
                                         1.0))
                    pe_idx = {'i': 0,
                              'j': 1,
                              'k': 2}[data['PhaseEncodingDirection'][0]]
                    npe = img.shape[pe_idx]
                    etl = npe // acc
                    ees = data.get('EffectiveEchoSpacing', None)
                    if ees is None:
                        raise Exception('Field "EffectiveEchoSpacing" not '
                                        'found in json')
                    trt = ees * (etl - 1)
                    data['TotalReadoutTime'] = trt

                if overwrite or ('TaskName' not in data.keys()):
                    data['TaskName'] = task

                if overwrite or ('TaskName' not in data.keys()) or \
                   ('TotalReadoutTime' not in data.keys()):
                    with open(json_fname, 'w') as f_obj:
                        json.dump(data, f_obj, sort_keys=True, indent=4)


def complete_dwi_jsons(bids_dir, subs, ses, overwrite):
    """
    bids_dir: path to BIDS dataset
    subs: list of subjects
    ses: string of session
    overwrite: bool

    Calculate TotalReadoutTime
    """
    layout = BIDSLayout(bids_dir)
    for sid in subs:
        if ses:
            niftis = layout.get(subject=sid, session=ses, modality='dwi',
                                extensions='nii.gz')
        else:
            niftis = layout.get(subject=sid, modality='dwi',
                                extensions='nii.gz')

        for nifti in niftis:
            nifti_fname = nifti.filename
            img = nib.load(nifti_fname)
            # get_nearest doesn't work with field maps atm
            data = layout.get_metadata(nifti_fname)
            json_fname = nifti_fname.replace('.nii.gz', '.json')

            if overwrite or 'TotalReadoutTime' not in data.keys():
                # This next bit taken shamelessly from fmriprep
                acc = float(data.get('ParallelReductionFactorInPlane', 1.0))
                pe_idx = {'i': 0,
                          'j': 1,
                          'k': 2}[data['PhaseEncodingDirection'][0]]
                npe = img.shape[pe_idx]
                etl = npe // acc
                ees = data.get('EffectiveEchoSpacing', None)
                if ees is None:
                    raise Exception('Field "EffectiveEchoSpacing" not '
                                    'found in json')
                trt = ees * (etl - 1)
                data['TotalReadoutTime'] = trt
                with open(json_fname, 'w') as f_obj:
                    json.dump(data, f_obj, sort_keys=True, indent=4)


def run(bids_dir, subs, ses, overwrite):
    complete_fmap_jsons(bids_dir, subs, ses, overwrite)
    complete_func_jsons(bids_dir, subs, ses, overwrite)
    complete_dwi_jsons(bids_dir, subs, ses, overwrite)


def main(args=None):
    docstr = __doc__
    parser = argparse.ArgumentParser(description=docstr)
    parser.add_argument('-d', '--bids_dir', dest='bids_dir', required=True,
                        type=str, help='location of BIDS dataset')
    parser.add_argument('-s', '--subjects', dest='subs', required=True,
                        type=str, nargs='+', help='list of subjects')
    parser.add_argument('-ss', '--ses', dest='session', required=False,
                        default=None,
                        help='session for longitudinal studies, default is '
                             'none')
    parser.add_argument('-o', '--overwrite', dest='overwrite', required=False,
                        default=False, action='store_true',
                        help='overwrite fmap jsons')
    args = parser.parse_args(args)
    if isinstance(args.session, str) and args.session == 'None':
        args.session = None
    run(args.bids_dir, args.subs, args.session, args.overwrite)


if __name__ == '__main__':
    main()
