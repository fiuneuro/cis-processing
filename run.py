#!/usr/bin/env python3
"""
From https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import json
import shutil
import tarfile
import argparse
import subprocess
from glob import glob

import pandas as pd


def run(command, env={}):
    merged_env = os.environ
    merged_env.update(env)
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, shell=True,
                               env=merged_env)
    while True:
        line = process.stdout.readline()
        line = str(line, 'utf-8')[:-1]
        print(line)
        if line == '' and process.poll() is not None:
            break

    if process.returncode != 0:
        raise Exception("Non zero return code: %d" % process.returncode)


def get_parser():
    parser = argparse.ArgumentParser(description='Run MRIQC on BIDS dataset.')
    parser.add_argument('-d', '--dicomdir', required=True, dest='dicom_dir',
                        help='Directory containing raw data.')
    parser.add_argument('-b', '--bidsdir', required=True, dest='bids_dir',
                        help=('Output directory for BIDS dataset and '
                              'derivatives.'))
    parser.add_argument('-w', '--workdir', required=False, dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dset_dir.')
    parser.add_argument('--config', required=True, dest='config',
                        help='Path to the config json file.')
    parser.add_argument('--project', required=True, dest='project',
                        help='The name of the project to analyze.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=False, dest='ses',
                        help='Session number', default=None)
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if args.work_dir is None:
        args.work_dir = CIS_DIR

    if args.ses is None:
        args.work_dir = op.join(args.work_dir,
                                '{0}-{1}'.format(args.project, args.sub))
    else:
        args.work_dir = op.join(args.work_dir,
                                '{0}-{1}-{2}'.format(args.project, args.sub,
                                                     args.ses))

    if not args.work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    if not op.isdir(args.dicom_dir):
        raise ValueError('Argument "dicom_dir" must be an existing directory.')

    if not op.isfile(args.config):
        raise ValueError('Argument "config" must be an existing file.')

    with open(args.config, 'r') as fo:
        config_options = json.load(fo)

    bidsifier_file = op.join('/home/data/nbc/singularity_images/',
                             config_options['bidsifier'])
    mriqc_file = op.join('/home/data/nbc/singularity_images/',
                         config_options['mriqc'])
    mriqc_version = mriqc_file.split('-')[0].split('_')[-1]

    out_deriv_dir = op.join(args.bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

    # Additional checks and copying for heuristics file
    heuristics_file = config_options['heuristics']
    if not heuristics_file.startswith('/'):
        heuristics_file = op.join('/home/data/nbc/cis_dataqc/cis-processing', heuristics_file)

    if not op.isfile(heuristics_file):
        raise ValueError('Heuristics file specified in config files must be '
                         'an existing file.')
    if not op.isfile(bidsifier_file):
        raise ValueError('BIDSifier image specified in config files must be '
                         'an existing file.')
    if not op.isfile(mriqc_file):
        raise ValueError('MRIQC image specified in config files must be '
                         'an existing file.')

    # Make folders/files
    os.makedirs(args.work_dir, exist_ok=True)
    os.makedirs(args.bids_dir, exist_ok=True)
    os.makedirs(out_deriv_dir, exist_ok=True)
    os.makedirs(op.join(out_deriv_dir, 'derivatives'), exist_ok=True)
    os.makedirs(op.join(out_deriv_dir, 'logs'), exist_ok=True)
    os.makedirs(op.join(out_deriv_dir, 'reports'), exist_ok=True)
    shutil.copyfile(heuristics_file,
                    op.join(args.work_dir, 'heuristics.py'))

    # Copy singularity images to scratch
    scratch_bidsifier = op.join(CIS_DIR, op.basename(bidsifier_file))
    scratch_mriqc = op.join(CIS_DIR, op.basename(mriqc_file))
    if not op.isfile(scratch_bidsifier):
        shutil.copyfile(bidsifier_file, scratch_bidsifier)

    if not op.isfile(scratch_mriqc):
        shutil.copyfile(mriqc_file, scratch_mriqc)

    # Temporary BIDS directory in work_dir
    scratch_bids_dir = op.join(args.work_dir, 'bids')
    scratch_deriv_dir = op.join(scratch_bids_dir, 'derivatives')
    mriqc_work_dir = op.join(args.work_dir, 'work')

    # Tar dicom folders into single file
    if args.ses is None:
        tarred_file = op.join(args.work_dir, 'sub-{0}.tar'.format(args.sub))
    else:
        tarred_file = op.join(args.work_dir,
                              'sub-{0}-ses-{1}.tar'.format(args.sub, args.ses))

    with tarfile.open(tarred_file, 'w') as tar:
        tar.add(args.dicom_dir)

    # Copy tar file to work_dir/
    shutil.copyfile(tarred_file, args.work_dir)

    # Run BIDSifier
    cmd = ('{sing} -d {work} --heuristics {heur} --project {proj} --sub {sub} '
           '--ses {ses}'.format(sing=scratch_bidsifier, work=args.work_dir,
                                heur=op.join(args.work_dir, 'heuristics.py'),
                                sub=args.sub, ses=args.ses, proj=args.project))
    print(cmd)

    # Check if BIDSification ran successfully
    bids_successful = False
    with open(op.join(args.work_dir, 'validator.txt'), 'r') as fo:
        validator_result = fo.read()

    if "This dataset appears to be BIDS compatible" in validator_result:
        bids_successful = True
    os.remove(op.join(args.work_dir, 'validator.txt'))

    if bids_successful:
        # Merge BIDS dataset into final folder
        dset_files = ['CHANGES', 'README', 'dataset_description.json',
                      'participants.tsv']
        for dset_file in dset_files:
            if not op.isfile(op.join(args.bids_dir, dset_file)):
                shutil.copyfile(op.join(args.work_dir, 'bids', dset_file),
                                op.join(args.bids_dir, dset_file))

        p_df = pd.read_csv(op.join(args.work_dir, 'bids/participants.tsv'),
                           sep='\t')
        p_df2 = pd.read_csv(op.join(args.bids_dir, 'participants.tsv'),
                            sep='\t')
        # Check if row already in participants file
        matches = p_df[(p_df == p_df2.loc[0]).all(axis=1)]
        match = matches.index.values.size
        if not match:
            p_df = pd.concat((p_df, p_df2))
            p_df.to_csv(op.join(args.work_dir, 'bids/participants.tsv'),
                        sep='\t', index=False)
        else:
            print('Subject/session already found in participants.tsv')

        scratch_sub_dir = op.join(args.work_dir,
                                  'bids/sub-{0}'.format(args.sub))
        out_sub_dir = op.join(args.bids_dir, 'sub-{0}')
        if not op.isdir(out_sub_dir):
            shutil.copy(scratch_sub_dir, out_sub_dir)
        elif args.ses is not None:
            scratch_ses_dir = op.join(scratch_sub_dir,
                                      'ses-{0}'.format(args.ses))
            out_ses_dir = op.join(out_sub_dir, 'ses-{0}'.format(args.ses))
            if not op.isdir(out_ses_dir):
                shutil.copy(scratch_ses_dir, out_ses_dir)
            else:
                print('Warning: Subject/session directory already exists in '
                      'dataset.')
        else:
            print('Warning: Subject directory already exists in dataset.')

        # Run MRIQC
        kwargs = ''
        for field in config_options['mriqc_settings'].keys():
            if isinstance(config_options['mriqc_settings'][field], list):
                val = ' '.join(config_options['mriqc_settings'][field])
            else:
                val = config_options['mriqc_settings'][field]
            kwargs += '--{0} {1} '.format(field, val)
        kwargs = kwargs.rstrip()
        cmd = ('{sing} {bids} {out} --no-sub --verbose-reports --ica '
               '--correct-slice-timing -w {work} '
               '{kwargs}'.format(sing=scratch_mriqc, bids=scratch_bids_dir,
                                 out=scratch_deriv_dir, work=mriqc_work_dir,
                                 kwargs=kwargs))
        print(cmd)

        # Merge MRIQC results into final derivatives folder
        reports = glob(op.join(scratch_deriv_dir, 'mriqc/reports/*.html'))
        reports = [f for f in reports if '_group' not in op.basename(f)]
        for report in reports:
            shutil.copy(report, op.join(out_deriv_dir, 'reports'))

        logs = glob(op.join(scratch_deriv_dir, 'mriqc/logs/*'))
        for log in logs:
            shutil.copy(log, op.join(out_deriv_dir, 'logs'))

        derivatives = glob(op.join(scratch_deriv_dir, 'mriqc/derivatives/*'))
        for derivative in derivatives:
            shutil.copy(derivative, op.join(out_deriv_dir, 'derivatives'))

        csv_files = glob(op.join(scratch_deriv_dir, 'mriqc/*.csv'))
        for csv_file in csv_files:
            out_file = op.join(out_deriv_dir, op.basename(csv_file))
            if not op.isfile(out_file):
                shutil.copyfile(csv_file, out_file)
            else:
                new_df = pd.read_csv(csv_file)
                old_df = pd.read_csv(out_file)
                out_df = pd.concat((old_df, new_df))
                out_df.to_csv(out_file, index=False)
    else:
        print('Heudiconv-generated dataset failed BIDS validator. '
              'Not running MRIQC')


if __name__ == '__main__':
    main()
