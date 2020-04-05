#!/usr/bin/env python3
"""
The full cis-processing workflow.
"""
import os
import os.path as op
import json
import shutil
from glob import glob
import getpass

import argparse
import pandas as pd

from utils import run


def _get_parser():
    parser = argparse.ArgumentParser(description='Run MRIQC on BIDS dataset.')
    parser.add_argument('-t', '--tarfile',
                        required=True,
                        dest='tar_file',
                        help='Tarred file containing raw (dicom) data.')
    parser.add_argument('-b', '--bidsdir',
                        required=True,
                        dest='bids_dir',
                        help='Output directory for BIDS dataset and '
                             'derivatives.')
    parser.add_argument('-w', '--workdir',
                        required=False,
                        dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dset_dir.')
    parser.add_argument('--config',
                        required=True,
                        dest='config',
                        help='Path to the config json file.')
    parser.add_argument('--sub',
                        required=True,
                        dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses',
                        required=False,
                        dest='ses',
                        help='Session number',
                        default=None)
    parser.add_argument('--n_procs',
                        required=False,
                        dest='n_procs',
                        help='Number of processes with which to run MRIQC.',
                        default=1,
                        type=int)
    return parser


def main(tar_file, bids_dir, config, sub, ses=None, work_dir=None, n_procs=1):
    """
    """
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

    if ses is None:
        scan_work_dir = op.join(work_dir,
                                '{0}-{1}'.format(config_options['project'], sub))
    else:
        scan_work_dir = op.join(work_dir,
                                '{0}-{1}-{2}'.format(config_options['project'], sub,
                                                     ses))

    if not scan_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    bidsifier_file = op.join('/home/data/cis/singularity-images/',
                             config_options['bidsifier'])
    mriqc_file = op.join('/home/data/cis/singularity-images/',
                         config_options['mriqc'])
    mriqc_version = op.basename(mriqc_file).split('-')[0].split('_')[-1].split('.sif')[0]

    out_deriv_dir = op.join(bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

    # Additional checks and copying for heuristics file
    heuristics_file = config_options['heuristics']
    if not heuristics_file.startswith('/'):
        heuristics_file = op.join(op.dirname(bids_dir), heuristics_file)

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
    if not op.isdir(scan_work_dir):
        os.makedirs(scan_work_dir)

    if not op.isdir(bids_dir):
        os.makedirs(bids_dir)

    shutil.copyfile(heuristics_file, op.join(scan_work_dir, 'heuristics.py'))

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
    if ses is None:
        work_tar_file = op.join(scan_work_dir, 'sub-{0}.tar'.format(sub))
    else:
        work_tar_file = op.join(scan_work_dir, 'sub-{0}-ses-{1}.tar'.format(sub, ses))
    shutil.copyfile(tar_file, work_tar_file)

    # Run BIDSifier
    cmd = ('{sing} -d {work} --heuristics {heur} --sub {sub} '
           '--ses {ses} -o {outdir}'.format(
                sing=scratch_bidsifier, work=work_tar_file,
                heur=op.join(scan_work_dir, 'heuristics.py'),
                sub=sub, ses=ses, outdir=op.join(scan_work_dir, 'bids')))
    run(cmd)

    # Check if BIDSification ran successfully
    bids_successful = False
    with open(op.join(scan_work_dir, 'bids', 'validator.txt'), 'r') as fo:
        validator_result = fo.read()

    if "This dataset appears to be BIDS compatible" in validator_result:
        bids_successful = True
    os.remove(op.join(scan_work_dir, 'bids', 'validator.txt'))

    if bids_successful:
        # Merge BIDS dataset into final folder
        dset_files = ['CHANGES', 'README', 'dataset_description.json',
                      'participants.tsv']
        for dset_file in dset_files:
            if not op.isfile(op.join(bids_dir, dset_file)):
                shutil.copyfile(op.join(scan_work_dir, 'bids', dset_file),
                                op.join(bids_dir, dset_file))

        new_participants_df = pd.read_csv(op.join(scan_work_dir, 'bids/participants.tsv'),
                                          sep='\t')
        new_participants_df = new_participants_df.T.drop_duplicates().T
        orig_participants_df = pd.read_csv(op.join(bids_dir, 'participants.tsv'),
                                           sep='\t')
        orig_participants_df = orig_participants_df.T.drop_duplicates().T

        # Check if row already in participants file
        matches = new_participants_df[
            (new_participants_df == orig_participants_df.loc[0]).all(axis=1)
        ]
        match = matches.index.values.size
        if not match:
            new_participants_df = pd.concat((new_participants_df, orig_participants_df))
            new_participants_df.to_csv(op.join(bids_dir, 'participants.tsv'),
                                       sep='\t', index=False)
        else:
            print('Subject/session already found in participants.tsv')

        scratch_sub_dir = op.join(scan_work_dir, 'bids/sub-{0}'.format(sub))
        out_sub_dir = op.join(bids_dir, 'sub-{0}'.format(sub))
        if not op.isdir(out_sub_dir):
            shutil.copytree(scratch_sub_dir, out_sub_dir)
        elif ses is not None:
            scratch_ses_dir = op.join(scratch_sub_dir, 'ses-{0}'.format(ses))
            out_ses_dir = op.join(out_sub_dir, 'ses-{0}'.format(ses))
            if not op.isdir(out_ses_dir):
                shutil.copytree(scratch_ses_dir, out_ses_dir)
            else:
                print('Warning: Subject/session directory already exists in dataset.')
        else:
            print('Warning: Subject directory already exists in dataset.')

        if ses is not None:
            sub_scans_df = pd.read_csv(
                op.join(
                    out_sub_dir,
                    'ses-{ses}/sub-{sub}_ses-{ses}_scans.tsv'.format(sub=sub, ses=ses)),
                sep='\t')
        else:
            sub_scans_df = pd.read_csv(
                op.join(
                    out_sub_dir,
                    'sub-{sub}_scans.tsv'.format(sub=sub)),
                sep='\t')

        # append scans.tsv file with remove and annot fields
        sub_scans_df['remove'] = 0
        sub_scans_df['annotation'] = ''

        # import master scans file
        master_scans_file = op.join(op.dirname(bids_dir),
                                    'code/{}_scans.tsv'.format(config_options['project']))
        if op.isfile(master_scans_file):
            master_scans_df = pd.read_csv(master_scans_file, sep='\t')
            master_df_headers = list(master_scans_df)
            master_scans_df = master_scans_df.append(sub_scans_df)
            master_scans_df.to_csv(master_scans_file, sep='\t', index=False,
                                   columns=master_df_headers)
        else:
            tmp_df_headers = list(sub_scans_df)
            sub_scans_df.to_csv(master_scans_file, sep='\t', index=False, columns=tmp_df_headers)

        # Run MRIQC
        if not op.isdir(op.join(work_dir, 'templateflow')):
            shutil.copytree('/home/data/cis/templateflow', op.join(work_dir, 'templateflow'))

        username = getpass.getuser()
        if not op.isdir(op.join('/home', username, '.cache/templateflow')):
            os.makedirs(op.join('/home', username, '.cache/templateflow'))

        if not op.isdir(out_deriv_dir):
            os.makedirs(out_deriv_dir)

        if not op.isdir(op.join(out_deriv_dir, 'logs')):
            os.makedirs(op.join(out_deriv_dir, 'logs'))

        # Run MRIQC anat
        for tmp_mod in config_options['mriqc_options']['anat']['mod'].keys():
            kwarg_str = ''
            settings_dict = config_options['mriqc_options']['anat']['mod'][
                tmp_mod]['mriqc_settings']
            for field in settings_dict.keys():
                if isinstance(settings_dict[field], list):
                    val = ' '.join(settings_dict[field])
                else:
                    val = settings_dict[field]
                kwarg_str += '--{0} {1} '.format(field, val)
            kwarg_str = kwarg_str.rstrip()
            cmd = ('singularity run --cleanenv '
                   '-B {templateflowdir}:$HOME/.cache/templateflow '
                   '{sing} {bids} {out} participant '
                   '--no-sub --verbose-reports '
                   '-m {mod} '
                   '-w {work} --n_procs {n_procs} '
                   '{kwarg_str}'.format(
                        templateflowdir=op.join(work_dir, 'templateflow'),
                        sing=scratch_mriqc,
                        bids=scratch_bids_dir,
                        out=scratch_deriv_dir,
                        mod=tmp_mod,
                        work=mriqc_work_dir,
                        n_procs=n_procs,
                        kwarg_str=kwarg_str))
            run(cmd)

        # Run MRIQC func
        for tmp_task in config_options['mriqc_options']['func']['task'].keys():
            if ses is not None:
                task_json_file = op.join(
                    scratch_bids_dir,
                    'sub-{sub}/ses-{ses}/func/sub-{sub}_ses-{ses}_'
                    'task-{task}_run-01_bold.json'.format(sub=sub, ses=ses, task=tmp_task))
                if op.isfile(task_json_file):
                    run_mriqc = True
                else:
                    run_mriqc = False
            else:
                task_json_file = op.join(
                    scratch_bids_dir,
                    'sub-{sub}/func/sub-{sub}_task-{task}_run-01_bold.'
                    'json'.format(sub=sub, task=tmp_task))
                if op.isfile(task_json_file):
                    run_mriqc = True
                else:
                    run_mriqc = False

            if run_mriqc:
                kwarg_str = ''
                settings_dict = config_options['mriqc_options']['func']['task'][
                    tmp_task]['mriqc_settings']
                for field in settings_dict.keys():
                    if isinstance(settings_dict[field], list):
                        val = ' '.join(settings_dict[field])
                    else:
                        val = settings_dict[field]
                    kwarg_str += '--{0} {1} '.format(field, val)
                kwarg_str = kwarg_str.rstrip()
                cmd = ('singularity run --cleanenv '
                       '-B {templateflowdir}:$HOME/.cache/templateflow '
                       '{sing} {bids} {out} participant '
                       '--no-sub --verbose-reports '
                       '--task-id {task} -m bold '
                       '-w {work} --n_procs {n_procs} --correct-slice-timing '
                       '{kwarg_str}'.format(
                            templateflowdir=op.join(work_dir, 'templateflow'),
                            sing=scratch_mriqc,
                            bids=scratch_bids_dir,
                            out=scratch_deriv_dir,
                            task=tmp_task,
                            work=mriqc_work_dir,
                            n_procs=n_procs,
                            kwarg_str=kwarg_str))
                run(cmd)

        # Merge MRIQC results into final derivatives folder
        reports = glob(op.join(scratch_deriv_dir, '*.html'))
        reports = [f for f in reports if 'group_' not in op.basename(f)]
        for report in reports:
            shutil.copy(report, op.join(out_deriv_dir, op.basename(report)))

        logs = glob(op.join(scratch_deriv_dir, 'logs/*'))
        for log in logs:
            shutil.copy(log, op.join(out_deriv_dir, 'logs', op.basename(log)))

        derivatives = glob(op.join(scratch_deriv_dir, 'sub-*'))
        derivatives = [x for x in derivatives if '.html' not in op.basename(x)]
        for derivative in derivatives:
            shutil.copytree(
                derivative,
                op.join(out_deriv_dir, op.basename(derivative))
            )

        csv_files = glob(op.join(scratch_deriv_dir, '*.csv'))
        for csv_file in csv_files:
            out_file = op.join(out_deriv_dir, op.basename(csv_file))
            if not op.isfile(out_file):
                shutil.copyfile(csv_file, out_file)
            else:
                new_df = pd.read_csv(csv_file)
                old_df = pd.read_csv(out_file)
                out_df = pd.concat((old_df, new_df))
                out_df.to_csv(out_file, index=False)

        # Finally, clean up working directory *if successful*
        shutil.rmtree(scan_work_dir)
    else:
        print('Heudiconv-generated dataset failed BIDS validator. '
              'Not running MRIQC')


def _main(argv=None):
    options = _get_parser().parse_args(argv)
    kwargs = vars(options)
    main(**kwargs)


if __name__ == '__main__':
    _main()
