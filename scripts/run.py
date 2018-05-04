#!/usr/bin/env python3
"""
From https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
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
    parser = argparse.ArgumentParser(description='BIDS conversion and '
                                                 'anonymization for the FIU '
                                                 'scanner.')
    parser.add_argument('-d', '--dicomdir', required=True, dest='dicom_dir',
                        help='Directory containing raw data.')
    parser.add_argument('-o', '--outdir', required=True, dest='output_dir',
                        help="Output directory for subject's BIDS dataset.")
    parser.add_argument('-w', '--workdir', required=False, dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dicom_dir.')
    parser.add_argument('--heuristics', required=True, dest='heuristics',
                        help='Path to the heuristics file.')
    parser.add_argument('--project', required=True, dest='project',
                        help='Name of the project.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=True, dest='ses',
                        help='Session number')
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    # Check inputs
    if not args.dicom_dir.startswith('/scratch'):
        raise ValueError('Dicom files must be in scratch.')

    if not args.output_dir.startswith('/scratch'):
        raise ValueError('Output directory must be in scratch.')

    if args.work_dir is None:
        # Assumes first three folders of dicom_dir (including root) are
        # /scratch/[username]
        par_dir = os.sep.join(args.dicom_dir.split(os.sep)[:3])
        args.work_dir = op.join(par_dir, 'work')

    if not args.work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    if not op.isdir(args.dicom_dir):
        raise ValueError('Argument "dicom_dir" must be an existing directory.')

    if not op.isfile(args.heuristics):
        raise ValueError('Argument "heuristics" must be an existing file.')

    # Check and create output and working directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.work_dir, exist_ok=True)

    # Compile and run command
    cmd = ('./bidsconvert.sh {0} {1} {2} {3} {4} '
           '{5} {6}'.format(args.dicom_dir, args.output_dir, args.work_dir,
                            args.heuristics, args.project, args.sub, args.ses))
    run(cmd)


if __name__ == '__main__':
    main()
