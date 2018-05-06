#!/usr/bin/env python3
"""
From https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import json
import shutil
import argparse
import subprocess


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
    parser.add_argument('--ses', required=True, dest='ses',
                        help='Session number')
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    # Check inputs
    if not args.output_dir.startswith('/scratch'):
        raise ValueError('Output directory must be in scratch.')

    if args.work_dir is None:
        args.work_dir = '/scratch/cis_dataqc/'

    args.work_dir = op.join(args.work_dir,
                            '{0}-{1}-{2}'.format(args.project, args.sub,
                                                 args.ses))

    if not args.work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    '''
    if not args.work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    if not op.isdir(args.dicom_dir):
        raise ValueError('Argument "dicom_dir" must be an existing directory.')

    if not op.isfile(args.config):
        raise ValueError('Argument "config" must be an existing file.')
    '''

    with open(args.config, 'r') as fo:
        config_options = json.load(fo)

    bidsifier_file = op.join('/home/data/nbc/singularity_images/',
                             config_options['bidsifier'])
    mriqc_file = op.join('/home/data/nbc/singularity_images/',
                         config_options['mriqc'])

    '''
    # Additional checks and copying for heuristics file
    if not op.isfile(config_options['heuristics']):
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
    shutil.copyfile(config_options['heuristics'],
                    op.join(args.work_dir, 'heuristics.py'))
    '''

    # TODO: tar dicoms
    # TODO: copy tar file to work_dir/

    # Run BIDSifier
    cmd = ('{sing} -d {work} --heuristics {heur} --project {proj} --sub {sub} '
           '--ses {ses}'.format(sing=bidsifier_file, work=args.work_dir,
                                heur=op.join(args.work_dir, 'heuristics.py'),
                                sub=args.sub, ses=args.ses, proj=args.project))
    print(cmd)

    # Temporary BIDS directory in work_dir
    scratch_bids_dir = op.join(args.work_dir, 'bids')
    scratch_deriv_dir = op.join(scratch_bids_dir, 'derivatives')
    mriqc_work_dir = op.join(args.work_dir, 'work')

    # Run merge_files and check if BIDSification ran successfully
    cmd = ('./merge_files.sh {work} {bids} {proj} {sub} '
           '{ses}'.format(work=args.work_dir, bids=args.bids_dir,
                          sub=args.sub, ses=args.ses, proj=args.project))
    print(cmd)

    # TODO: Check success of BIDSification. Only run MRIQC if successful.
    # Run MRIQC
    kwargs = ''
    for field in config_options['mriqc_settings'].keys():
        if isinstance(config_options['mriqc_settings'][field], list):
            val = ' '.join(config_options['mriqc_settings'][field])
        else:
            val = config_options['mriqc_settings'][field]
        kwargs += '--{0} {1} '.format(field, val)
    kwargs = kwargs.rstrip()
    cmd = ('{sing} {bids} {out} participant --participant-label '
           '{sub} --session-id {ses} --no-sub --verbose-reports --ica '
           '--correct-slice-timing -w {work} '
           '{kwargs}'.format(sing=mriqc_file, bids=scratch_bids_dir,
                             out=scratch_deriv_dir, sub=args.sub,
                             ses=args.ses, work=mriqc_work_dir,
                             kwargs=kwargs))
    print(cmd)

    # TODO: Copy scratch BIDS directory back to real BIDS directory
    # TODO: Copy scratch derivatives to real derivatives


if __name__ == '__main__':
    main()
