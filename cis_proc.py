#!/usr/bin/env python3
"""
Based on
https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import json
import shutil
import tarfile
import subprocess
from glob import glob

import argparse
import pandas as pd


def run(command, env={}):
    merged_env = os.environ
    merged_env.update(env)
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, shell=True,
                               env=merged_env)
    while True:
        line = process.stdout.readline()
        #line = str(line).encode('utf-8')[:-1]
        line = str(line, 'utf-8')[:-1]
        print(line)
        if line == '' and process.poll() is not None:
            break

    if process.returncode != 0:
        raise Exception("Non zero return code: {0}\n"
                        "{1}\n\n{2}".format(process.returncode, command,
                                            process.stdout.read()))


def get_parser():
    parser = argparse.ArgumentParser(description='Run MRIQC on BIDS dataset.')
    parser.add_argument('-t', '--tarfile', required=True, dest='tar_file',
                        help='Tarred file containing raw (dicom) data.')
    parser.add_argument('-b', '--bidsdir', required=True, dest='bids_dir',
                        help=('Output directory for BIDS dataset and '
                              'derivatives.'))
    parser.add_argument('-w', '--workdir', required=False, dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dset_dir.')
    parser.add_argument('--config', required=True, dest='config',
                        help='Path to the config json file.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=False, dest='ses',
                        help='Session number', default=None)
    parser.add_argument('--n_procs', required=False, dest='n_procs',
                        help='Number of processes with which to run MRIQC.',
                        default=1, type=int)
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if args.work_dir is None:
        args.work_dir = CIS_DIR

    if not op.isfile(args.tar_file) or not args.tar_file.endswith('.tar'):
        raise ValueError('Argument "tar_file" must be an existing file with '
                         'the suffix ".tar".')

    if not op.isfile(args.config):
        raise ValueError('Argument "config" must be an existing file.')

    if args.n_procs < 1:
        raise ValueError('Argument "n_procs" must be positive integer greater '
                         'than zero.')
    n_procs = args.n_procs

    with open(args.config, 'r') as fo:
        config_options = json.load(fo)

    if 'project' not in config_options.keys():
        raise Exception('Config File must be updated with project field'
                        'See Sample Config File for More information')

    if args.ses is None:
        scan_work_dir = op.join(args.work_dir,
                                '{0}-{1}'.format(config_options['project'], args.sub))
    else:
        scan_work_dir = op.join(args.work_dir,
                                '{0}-{1}-{2}'.format(config_options['project'], args.sub,
                                                     args.ses))

    if not scan_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    bidsifier_file = op.join('/home/data/cis/singularity-images/',
                             config_options['bidsifier'])
    mriqc_file = op.join('/home/data/cis/singularity-images/',
                         config_options['mriqc'])
    mriqc_version = op.basename(mriqc_file).split('-')[0].split('_')[-1]

    out_deriv_dir = op.join(args.bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

    # Additional checks and copying for heuristics file
    heuristics_file = config_options['heuristics']
    if not heuristics_file.startswith('/'):
        heuristics_file = op.join(os.path.dirname(args.bids_dir), heuristics_file)

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

    if not op.isdir(args.bids_dir):
        os.makedirs(args.bids_dir)

    shutil.copyfile(heuristics_file,
                    op.join(scan_work_dir, 'heuristics.py'))

    # Copy singularity images to scratch
    scratch_bidsifier = op.join(CIS_DIR, op.basename(bidsifier_file))
    scratch_mriqc = op.join(CIS_DIR, op.basename(mriqc_file))
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
    if args.ses is None:
        work_tar_file = op.join(scan_work_dir, 'sub-{0}.tar'.format(args.sub))
    else:
        work_tar_file = op.join(scan_work_dir,
                                'sub-{0}-ses-{1}.tar'.format(args.sub,
                                                             args.ses))
    shutil.copyfile(args.tar_file, work_tar_file)

    # Run BIDSifier
    cmd = ('{sing} -d {work} --heuristics {heur} --project {proj} --sub {sub} '
           '--ses {ses}'.format(sing=scratch_bidsifier, work=scan_work_dir,
                                heur=op.join(scan_work_dir, 'heuristics.py'),
                                sub=args.sub, ses=args.ses, proj=config_options['project']))
    run(cmd)

    # Check if BIDSification ran successfully
    bids_successful = False
    with open(op.join(scan_work_dir, 'validator.txt'), 'r') as fo:
        validator_result = fo.read()

    if "This dataset appears to be BIDS compatible" in validator_result:
        bids_successful = True
    os.remove(op.join(scan_work_dir, 'validator.txt'))

    if bids_successful:
        # Merge BIDS dataset into final folder
        dset_files = ['CHANGES', 'README', 'dataset_description.json',
                      'participants.tsv']
        for dset_file in dset_files:
            if not op.isfile(op.join(args.bids_dir, dset_file)):
                shutil.copyfile(op.join(scan_work_dir, 'bids', dset_file),
                                op.join(args.bids_dir, dset_file))

        p_df = pd.read_csv(op.join(scan_work_dir, 'bids/participants.tsv'),
                           sep='\t')
        p_df = p_df.T.drop_duplicates().T
        p_df2 = pd.read_csv(op.join(args.bids_dir, 'participants.tsv'),
                            sep='\t')
        p_df2 = p_df2.T.drop_duplicates().T

        # Check if row already in participants file
        matches = p_df[(p_df == p_df2.loc[0]).all(axis=1)]
        match = matches.index.values.size
        if not match:
            p_df = pd.concat((p_df, p_df2))
            p_df.to_csv(op.join(args.bids_dir, 'participants.tsv'),
                        sep='\t', index=False)
        else:
            print('Subject/session already found in participants.tsv')

        scratch_sub_dir = op.join(scan_work_dir,
                                  'bids/sub-{0}'.format(args.sub))
        out_sub_dir = op.join(args.bids_dir, 'sub-{0}'.format(args.sub))
        if not op.isdir(out_sub_dir):
            shutil.copytree(scratch_sub_dir, out_sub_dir)
        elif args.ses is not None:
            scratch_ses_dir = op.join(scratch_sub_dir,
                                      'ses-{0}'.format(args.ses))
            out_ses_dir = op.join(out_sub_dir, 'ses-{0}'.format(args.ses))
            if not op.isdir(out_ses_dir):
                shutil.copytree(scratch_ses_dir, out_ses_dir)

            else:
                print('Warning: Subject/session directory already exists in '
                      'dataset.')
        else:
            print('Warning: Subject directory already exists in dataset.')

        if args.ses is not None:
            tmp_df = pd.read_csv(op.join(out_sub_dir, 'ses-{ses}'.format(ses=args.ses), 'sub-{sub}_ses-{ses}_scans.tsv'.format(sub=args.sub, ses=args.ses)), sep='\t')
        else:
            tmp_df = pd.read_csv(op.join(out_sub_dir, 'sub-{sub}_scans.tsv'.format(sub=args.sub)), sep='\t')

        #append scans.tsv file with remove and annot fields
        tmp_df['remove'] = 0
        tmp_df['annotation'] = ''

        #import master scans file
        if op.isfile(op.join(os.path.dirname(args.bids_dir), 'code/{proj}_scans.tsv'.format(proj=config_options['project']))):
            master_df = pd.read_csv(op.join(os.path.dirname(args.bids_dir), 'code/{proj}_scans.tsv'.format(proj=config_options['project'])), sep='\t')
            master_df_headers = list(master_df)
            master_df = master_df.append(tmp_df)
            master_df.to_csv(op.join(os.path.dirname(args.bids_dir), 'code/{proj}_scans.tsv'.format(proj=config_options['project'])), sep='\t', index=False, columns=master_df_headers)
        else:
            tmp_df_headers = list(tmp_df)
            tmp_df.to_csv(op.join(os.path.dirname(args.bids_dir), 'code/{proj}_scans.tsv'.format(proj=config_options['project'])), sep='\t', index=False, columns=tmp_df_headers)

        # Run MRIQC
        if not op.isdir(out_deriv_dir):
            os.makedirs(out_deriv_dir)

        if not op.isdir(op.join(out_deriv_dir, 'derivatives')):
            os.makedirs(op.join(out_deriv_dir, 'derivatives'))

        if not op.isdir(op.join(out_deriv_dir, 'logs')):
            os.makedirs(op.join(out_deriv_dir, 'logs'))

        if not op.isdir(op.join(out_deriv_dir, 'reports')):
            os.makedirs(op.join(out_deriv_dir, 'reports'))

        # Run MRIQC anat
        for tmp_mod in config_options['mriqc_options']['anat']['mod'].keys():
            kwargs = ''
            for field in config_options['mriqc_options']['anat']['mod'][tmp_mod]['mriqc_settings'].keys():
                if isinstance(config_options['mriqc_options']['anat']['mod'][tmp_mod]['mriqc_settings'][field], list):
                    val = ' '.join(config_options['mriqc_options']['anat']['mod'][tmp_mod]['mriqc_settings'][field])
                else:
                    val = config_options['mriqc_options']['anat']['mod'][tmp_mod]['mriqc_settings'][field]
                kwargs += '--{0} {1} '.format(field, val)
            kwargs = kwargs.rstrip()
            cmd = ('{sing} {bids} {out} participant --no-sub --verbose-reports '
                   '-m {mod} '
                   '-w {work} --n_procs {n_procs} '
                   '{kwargs} '.format(sing=scratch_mriqc, bids=scratch_bids_dir,
                                      out=scratch_deriv_dir, mod=tmp_mod,
                                      work=mriqc_work_dir, n_procs=n_procs, kwargs=kwargs))
            run(cmd)

        # Run MRIQC func
        for tmp_task in config_options['mriqc_options']['func']['task'].keys():
            print(op.join(scratch_bids_dir, 'sub-{sub}'.format(sub=args.sub), 'ses-{ses}'.format(ses=args.ses), 'func/sub-{sub}_ses-{ses}_task-{task}_run-01_bold.json'.format(sub=args.sub, ses=args.ses, task=tmp_task)))
            if args.ses:
                run_mriqc = op.isfile(op.join(scratch_bids_dir, 'sub-{sub}'.format(sub=args.sub), 'ses-{ses}'.format(ses=args.ses), 'func/sub-{sub}_ses-{ses}_task-{task}_run-01_bold.json'.format(sub=args.sub, ses=args.ses, task=tmp_task)))

            else:
                run_mriqc = op.isfile(op.join(scratch_bids_dir, 'sub-{sub}'.format(sub=args.sub),
                                              'func/sub-{sub}_task-{task}_run-01_bold.json'.format(sub=args.sub,
                                                                                                   task=tmp_task)))


            if run_mriqc:
                kwargs = ''
                for field in config_options['mriqc_options']['func']['task'][tmp_task]['mriqc_settings'].keys():
                    if isinstance(config_options['mriqc_options']['func']['task'][tmp_task]['mriqc_settings'][field], list):
                        val = ' '.join(config_options['mriqc_options']['func']['task'][tmp_task]['mriqc_settings'][field])
                    else:
                        val = config_options['mriqc_options']['func']['task'][tmp_task]['mriqc_settings'][field]
                    kwargs += '--{0} {1} '.format(field, val)
                kwargs = kwargs.rstrip()
                cmd = ('{sing} {bids} {out} participant --no-sub --verbose-reports '
                       '--task-id {task} -m bold '
                       '-w {work} --n_procs {n_procs} --correct-slice-timing '
                       '{kwargs} '.format(sing=scratch_mriqc, bids=scratch_bids_dir,
                                          out=scratch_deriv_dir, task=tmp_task,
                                          work=mriqc_work_dir, n_procs=n_procs, kwargs=kwargs))
                run(cmd)

        # Merge MRIQC results into final derivatives folder
        reports = glob(op.join(scratch_deriv_dir, 'reports/*.html'))
        reports = [f for f in reports if '_group' not in op.basename(f)]
        for report in reports:
            shutil.copy(report, op.join(out_deriv_dir, 'reports',
                                        op.basename(report)))

        logs = glob(op.join(scratch_deriv_dir, 'logs/*'))
        for log in logs:
            shutil.copy(log, op.join(out_deriv_dir, 'logs', op.basename(log)))

        derivatives = glob(op.join(scratch_deriv_dir, 'derivatives/*'))
        for derivative in derivatives:
            shutil.copy(derivative, op.join(out_deriv_dir, 'derivatives',
                                            op.basename(derivative)))

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


if __name__ == '__main__':
    main()
