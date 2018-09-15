#!/usr/bin/env python3
"""
From https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import tarfile
import argparse
import subprocess

import pydicom
import numpy as np
import pandas as pd
from dateutil.parser import parse


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
        raise Exception("Non zero return code: {0}\n"
                        "{1}\n\n{2}".format(process.returncode, command,
                                            process.stdout.read()))


def get_parser():
    parser = argparse.ArgumentParser(description='BIDS conversion and '
                                                 'anonymization for the FIU '
                                                 'scanner.')
    parser.add_argument('-d', '--dicomdir', required=True, dest='dicom_dir',
                        help='Directory containing raw data.')
    parser.add_argument('--heuristics', required=True, dest='heuristics',
                        help='Path to the heuristics file.')
    parser.add_argument('--project', required=True, dest='project',
                        help='Name of the project.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=False, dest='ses',
                        help='Session number', default=None)
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    # Check inputs
    if args.ses is None:
        tar_file = op.join(args.dicom_dir, 'sub-{0}.tar'.format(args.sub))
    else:
        tar_file = op.join(args.dicom_dir,
                           'sub-{0}-ses-{1}.tar'.format(args.sub, args.ses))

    if not args.dicom_dir.startswith('/scratch'):
        raise ValueError('Dicom files must be in scratch.')

    if not op.isfile(tar_file):
        raise ValueError('Argument "dicom_dir" must contain a file '
                         'named {0}.'.format(op.basename(tar_file)))

    if not op.isfile(args.heuristics):
        raise ValueError('Argument "heuristics" must be an existing file.')

    # Compile and run command
    cmd = ('/scripts/bidsconvert.sh {0} {1} {2} {3} {4}'.format(args.dicom_dir,
                                                                args.heuristics,
                                                                args.project,
                                                                args.sub, args.ses))
    run(cmd, env={'TMPDIR': args.dicom_dir})

    # Grab some info to add to the participants file
    participants_file = op.join(args.dicom_dir, 'bids/participants.tsv')
    if op.isfile(participants_file):
        df = pd.read_csv(participants_file, sep='\t')
        with tarfile.open(tar_file, 'r') as tar:
            dicoms = [mem for mem in tar.getmembers() if
                      mem.name.endswith('.dcm')]
            f_obj = tar.extractfile(dicoms[0])
            data = pydicom.read_file(f_obj)

        if data.get('PatientAge'):
            age = int(data.PatientAge.replace('Y', ''))
        elif data.get('PatientBirthDate'):
            age = parse(data.StudyDate) - parse(data.PatientBirthDate)
            age = np.round(age.days / 365.25, 2)
        else:
            age = np.nan
        df2 = pd.DataFrame(columns=['age', 'sex', 'weight'],
                           data=[[age, data.PatientSex, data.PatientWeight]])
        df = pd.concat((df, df2), axis=1)
        df.to_csv(participants_file, sep='\t', index=False)


if __name__ == '__main__':
    main()
