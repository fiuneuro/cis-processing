#!/usr/bin/env python3
"""The full cis-processing workflow."""
import os
import os.path as op
import re
import json
import shutil
import getpass

import argparse

from utils import run
from dataset import merge_datasets
from mriqc import run_mriqc, merge_mriqc_derivatives


def _get_parser():
    parser = argparse.ArgumentParser(description='Run MRIQC on BIDS dataset.')
    parser.add_argument(
        '-t', '--tarfile',
        required=True,
        dest='tar_file',
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
        '--n_procs',
        required=False,
        dest='n_procs',
        help='Number of processes with which to run MRIQC.',
        default=1,
        type=int)
    return parser


def main(tar_file, bids_dir, config, sub, ses=None, work_dir=None, n_procs=1):
    """Runtime for cis_proc.py."""
    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if work_dir is None:
        work_dir = CIS_DIR

    if not op.isfile(tar_file) or not tar_file.endswith('.tar'):
        raise ValueError('Argument "tar_file" must be an existing file with '
                         'the suffix ".tar".')

    if not op.isfile(config):
        raise ValueError('Argument "config" must be an existing file.')

    if n_procs < 1:
        raise ValueError('Argument "n_procs" must be positive integer greater '
                         'than zero.')
    else:
        n_procs = int(n_procs)

    with open(config, 'r') as fo:
        config_options = json.load(fo)

    if 'project' not in config_options.keys():
        raise Exception('Config File must be updated with project field'
                        'See Sample Config File for More information')

    scan_work_dir = op.join(
        work_dir, '{0}-{1}'.format(config_options['project'], sub))
    if ses:  # If ses is specified, append -<ses-name> to workdir
        scan_work_dir += '-{0}'.format(ses)

    if not scan_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    bidsifier_file = op.join('/home/data/cis/singularity-images/',
                             config_options['bidsifier'])
    mriqc_file = op.join('/home/data/cis/singularity-images/',
                         config_options['mriqc'])
    mriqc_version = re.search(r'_([\d.]+)', mriqc_file).group(1)

    out_deriv_dir = op.join(bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

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

    if not op.isfile(bidsifier_file):
        raise ValueError('BIDSifier image specified in config files must be '
                         'an existing file.')
    if not op.isfile(mriqc_file):
        raise ValueError('MRIQC image specified in config files must be '
                         'an existing file.')

    # Make folders/files
    if not op.isdir(scan_work_dir):
        os.makedirs(scan_work_dir)

    if not op.isdir(bids_dir):
        os.makedirs(bids_dir)

    scratch_heuristic = op.join(scan_work_dir, 'heuristic.py')
    shutil.copyfile(heuristic, scratch_heuristic)

    # Copy singularity images to scratch
    scratch_bidsifier = op.join(scan_work_dir, op.basename(bidsifier_file))
    scratch_mriqc = op.join(scan_work_dir, op.basename(mriqc_file))
    if not op.isfile(scratch_bidsifier):
        shutil.copyfile(bidsifier_file, scratch_bidsifier)
        os.chmod(scratch_bidsifier, 0o775)

    if not op.isfile(scratch_mriqc):
        shutil.copyfile(mriqc_file, scratch_mriqc)
        os.chmod(scratch_mriqc, 0o775)

    # Temporary BIDS directory in work_dir
    scratch_bids_dir = op.join(scan_work_dir, 'bids')
    scratch_deriv_dir = op.join(scratch_bids_dir, 'derivatives')
    mriqc_work_dir = op.join(scan_work_dir, 'work')

    # Copy tar file to work_dir
    work_tar_file = op.join(scan_work_dir, 'sub-{0}.tar'.format(sub))
    if ses:  # If session is specified, replace .tar and add -ses-<session>.tar
        work_tar_file = work_tar_file.replace(
            '.tar', '-ses-{0}.tar'.format(ses))
    shutil.copyfile(tar_file, work_tar_file)

    # Run BIDSifier
    cmd = ('{sing} -d {work} --heuristic {heur} --sub {sub} '
           '--ses {ses} -o {outdir}'.format(
               sing=scratch_bidsifier, work=work_tar_file,
               heur=scratch_heuristic,
               sub=sub, ses=ses, outdir=op.join(scan_work_dir, 'bids')))
    run(cmd)

    # Check if BIDSification ran successfully
    bids_successful = False
    with open(op.join(scratch_bids_dir, 'validator.txt'), 'r') as fo:
        validator_result = fo.read()

    if "This dataset appears to be BIDS compatible" in validator_result:
        bids_successful = True
    os.remove(op.join(scan_work_dir, 'bids', 'validator.txt'))

    if not bids_successful:
        raise RuntimeError('Heudiconv-generated dataset failed BIDS validator. '
                           'Not running MRIQC')

    # Run MRIQC
    if not op.isdir(op.join(work_dir, 'templateflow')):
        shutil.copytree(
            '/home/data/cis/templateflow',
            op.join(work_dir, 'templateflow'))

    username = getpass.getuser()
    if not op.isdir(op.join('/home', username, '.cache/templateflow')):
        os.makedirs(op.join('/home', username, '.cache/templateflow'))

    if not op.isdir(out_deriv_dir):
        os.makedirs(out_deriv_dir)

    if not op.isdir(op.join(out_deriv_dir, 'logs')):
        os.makedirs(op.join(out_deriv_dir, 'logs'))

    # If BIDSification was successful, merge new files into full BIDS dataset
    merge_datasets(scratch_bids_dir, bids_dir, config_options['project'], sub, ses)

    # MRIQC time
    if not op.isdir(out_deriv_dir):
        os.makedirs(out_deriv_dir)

    if not op.isdir(op.join(out_deriv_dir, 'logs')):
        os.makedirs(op.join(out_deriv_dir, 'logs'))

    if not op.isdir(op.join(work_dir, 'templateflow')):
        shutil.copytree('/home/data/cis/templateflow', op.join(work_dir, 'templateflow'))

    username = getpass.getuser()
    templateflow_dir = op.join('/home', username, '.cache/templateflow')
    if not op.isdir(templateflow_dir):
        os.makedirs(templateflow_dir)

    run_mriqc(bids_dir, templateflow_dir, scratch_mriqc, mriqc_work_dir,
              scratch_deriv_dir, config_options, sub=None, ses=None, n_procs=1)
    merge_mriqc_derivatives(scratch_deriv_dir, out_deriv_dir)

    # Finally, clean up working directory *if successful*
    shutil.rmtree(scan_work_dir)


def _main(argv=None):
    options = _get_parser().parse_args(argv)
    kwargs = vars(options)
    main(**kwargs)


if __name__ == '__main__':
    _main()
