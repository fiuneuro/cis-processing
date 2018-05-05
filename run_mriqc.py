#!/usr/bin/env python3
"""
From https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import json
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
    parser.add_argument('-d', '--bidsdir', required=True, dest='bids_dir',
                        help='Directory containing BIDS dataset.')
    parser.add_argument('-o', '--outdir', required=True, dest='output_dir',
                        help="Output directory for MRIQC derivatives.")
    parser.add_argument('-w', '--workdir', required=False, dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dset_dir.')
    parser.add_argument('--config', required=True, dest='config',
                        help='Path to the config json file.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=True, dest='ses',
                        help='Session number')
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    '''
    # Check inputs
    if not args.bids_dir.startswith('/scratch'):
        raise ValueError('BIDS dataset must be in scratch.')

    if not args.output_dir.startswith('/scratch'):
        raise ValueError('Output directory must be in scratch.')
    '''

    if args.work_dir is None:
        # Assumes first three folders of dicom_dir (including root) are
        # /scratch/[username]
        par_dir = os.sep.join(args.bids_dir.split(os.sep)[:3])
        args.work_dir = op.join(par_dir, 'work')

    '''

    if not args.work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    if not op.isdir(args.bids_dir):
        raise ValueError('Argument "bids_dir" must be an existing directory.')

    if not op.isfile(args.config):
        raise ValueError('Argument "config" must be an existing file.')

    # Check and create output and working directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.work_dir, exist_ok=True)
    '''

    with open(args.config, 'r') as fo:
        config_options = json.load(fo)

    # Run MRIQC
    mriqc_file = op.join('/home/data/nbc/singularity_images/',
                         config_options['mriqc'])
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
           '{kwargs}'.format(sing=mriqc_file, bids=args.bids_dir,
                             out=args.output_dir, sub=args.sub,
                             ses=args.ses, work=args.work_dir,
                             kwargs=kwargs))
    print(cmd)


if __name__ == '__main__':
    main()
