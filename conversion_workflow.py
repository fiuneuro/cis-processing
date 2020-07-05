#!/usr/bin/env python3
"""The full cis-processing workflow.

This workflow does the following:
1. Copy raw data tarball to scratch.
2. Copy necessary Singularity images to scratch.
3. Run BIDSifier Singularity image on tarball.
4. Merge mini BIDS dataset in /scratch into main BIDS dataset in /data.
5. Run MRIQC Singularity image on new mini-BIDS dataset.
6. Merge MRIQC derivatives in /scratch into main derivatives folder in /data.
7. Clean up working directory in /scratch.
"""
import os
import os.path as op
import re
import json
import shutil
import getpass

import argparse

from utils import run
from mriqc import run_mriqc


def _get_parser():
    parser = argparse.ArgumentParser(
        description='Convert incoming data to BIDS format and run MRIQC on '
                    'it with a set of Singularity images.')
    parser.add_argument(
        '-t', '--tarball',
        required=True,
        dest='tarball',
        help='Tarred file containing raw (dicom) data.')
    parser.add_argument(
        '-b', '--bidsdir',
        required=True,
        dest='bids_dir',
        help='Output directory for BIDS dataset and '
             'derivatives.')
    parser.add_argument(
        '-w', '--workdir',
        required=False,
        dest='work_dir',
        default=None,
        help='Path to a working directory. Defaults to work '
             'subfolder in dset_dir.')
    parser.add_argument(
        '--config',
        required=True,
        dest='config',
        help='Path to the config json file.')
    parser.add_argument(
        '--sub',
        required=True,
        dest='sub',
        help='The label of the subject to analyze.')
    parser.add_argument(
        '--ses',
        required=False,
        dest='ses',
        help='Session number',
        default=None)
    parser.add_argument(
        '--datalad',
        required=False,
        action='store_true',
        dest='datalad',
        help='Whether to use datalad to track changes or not.',
        default=False)
    return parser


def main(tarball, bids_dir, config, sub, ses=None, work_dir=None, datalad=False):
    """Runtime for conversion_workflow.py."""
    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if work_dir is None:
        work_dir = CIS_DIR

    if not op.isfile(tarball) or not tarball.endswith('.tar'):
        raise ValueError('Argument "tarball" must be an existing file with '
                         'the suffix ".tar".')

    if not op.isfile(config):
        raise ValueError('Argument "config" must be an existing file.')

    with open(config, 'r') as fo:
        config_options = json.load(fo)

    if 'project' not in config_options.keys():
        raise Exception('Config file must include a "project" field. '
                        'See sample config file for more information')

    scan_work_dir = op.join(
        work_dir,
        '{0}-{1}-{2}'.format(config_options['project'], sub, ses)
    )

    if not scan_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    singularity_dir = '/home/data/cis/singularity-images/'
    bidsifier_file = op.join(singularity_dir, config_options['bidsifier'])
    mriqc_file = op.join(singularity_dir, config_options['mriqc'])
    mriqc_version = re.search(r'_([\d.]+)', mriqc_file).group(1)
    mriqc_out_dir = op.join(bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

    if not op.isfile(bidsifier_file):
        raise ValueError('BIDSifier image specified in config file must be '
                         'an existing file.')
    if not op.isfile(mriqc_file):
        raise ValueError('MRIQC image specified in config file must be '
                         'an existing file.')

    # Make folders/files
    if not op.isdir(scan_work_dir):
        os.makedirs(scan_work_dir)

    if not op.isdir(bids_dir):
        os.makedirs(bids_dir)

    # Change directory to parent folder of bids_dir to give Singularity images
    # access to relevant directories.
    os.chdir(op.dirname(bids_dir))

    # Additional checks and copying for heuristic file
    heuristic = config_options['heuristic']

    # Heuristic may be file (absolute or relative path) or heudiconv builtin
    # Use existence of file extension to determine if builtin or file
    if op.splitext(heuristic)[1]:
        if not heuristic.startswith('/'):
            heuristic = op.join(op.dirname(bids_dir), heuristic)

        if not op.isfile(heuristic):
            raise ValueError('Heuristic file specified in config files must be '
                             'an existing file.')
        scratch_heuristic = op.join(scan_work_dir, 'heuristic.py')
        shutil.copyfile(heuristic, scratch_heuristic)
    else:
        scratch_heuristic = heuristic

    # Copy singularity images to scratch
    scratch_bidsifier = op.join(scan_work_dir, op.basename(bidsifier_file))
    scratch_mriqc = op.join(scan_work_dir, op.basename(mriqc_file))

    if not op.isfile(scratch_bidsifier):
        shutil.copyfile(bidsifier_file, scratch_bidsifier)
        os.chmod(scratch_bidsifier, 0o775)

    if not op.isfile(scratch_mriqc):
        shutil.copyfile(mriqc_file, scratch_mriqc)
        os.chmod(scratch_mriqc, 0o775)

    mriqc_work_dir = op.join(scan_work_dir, 'work')

    # Copy tar file to work_dir
    work_tar_file = op.join(scan_work_dir, 'sub-{0}.tar'.format(sub))
    if ses:  # If session is specified, replace .tar and add -ses-<session>.tar
        work_tar_file = work_tar_file.replace(
            '.tar', '-ses-{0}.tar'.format(ses))
    shutil.copyfile(tarball, work_tar_file)

    # Run BIDSifier
    cmd = ('{sing} -d {input} --heuristic {heur} --sub {sub} '
           '--ses {ses} -o {outdir} -w {workdir} {datalad_flag}'.format(
               sing=scratch_bidsifier, input=work_tar_file,
               heur=scratch_heuristic,
               sub=sub, ses=ses, outdir=bids_dir, workdir=scan_work_dir,
               datalad_flag='--datalad' if datalad else ''))
    run(cmd)

    # Check if BIDSification ran successfully
    bids_successful = False
    with open(op.join(scan_work_dir, 'validator.txt'), 'r') as fo:
        validator_result = fo.read()

    if 'This dataset appears to be BIDS compatible' in validator_result:
        bids_successful = True

    if not bids_successful:
        raise RuntimeError('Heudiconv-generated dataset failed BIDS validator. '
                           'Not running MRIQC')

    # MRIQC time
    if not op.isdir(mriqc_out_dir):
        os.makedirs(mriqc_out_dir)

    if not op.isdir(op.join(work_dir, 'templateflow')):
        shutil.copytree('/home/data/cis/templateflow', op.join(work_dir, 'templateflow'))

    username = getpass.getuser()
    templateflow_dir = op.join('/home', username, '.cache/templateflow')
    if not op.isdir(templateflow_dir):
        os.makedirs(templateflow_dir)

    run_mriqc(bids_dir=bids_dir, templateflow_dir=templateflow_dir,
              mriqc_singularity=scratch_mriqc, work_dir=mriqc_work_dir,
              out_dir=mriqc_out_dir,
              mriqc_config=config_options['mriqc_settings'],
              sub=sub, ses=ses)

    # Finally, clean up working directory *if successful*
    shutil.rmtree(scan_work_dir)


def _main(argv=None):
    options = _get_parser().parse_args(argv)
    kwargs = vars(options)
    main(**kwargs)


if __name__ == '__main__':
    _main()
