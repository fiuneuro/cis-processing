#!/usr/bin/env python3
"""A wrapper around conversion_workflow.py and cis-xget.

This workflow does the following:
1. Copy XNAT downloader Singularity image to scratch.
2. Download tarball using XNAT downloader.
3. Run protocol check on downloaded data.
4. Email project-related personnel warnings about missing data based on protocol check.
5. Submit conversion_workflow as a job.
6. Email project-related personnel update about downloaded/converted data.

Because the workflow downloads data from XNAT (which requires internet access),
it cannot be called within a SLURM job, as none of the processing nodes have
internet access. The workflow is thus called on the login or visualization
nodes, but submits the conversion_workflow step as a job to the processing
nodes.
"""
import os
import os.path as op
import json
import shutil
import tarfile
import datetime

import argparse
import pandas as pd

from utils import run


def _get_parser():
    parser = argparse.ArgumentParser(
        description='Initiate XNAT download and conversion workflow.')
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
        '--protocol_check',
        required=False,
        action='store_true',
        help='Will perform a protocol check to determine if '
             'the correct number of scans and TRs are present.')
    parser.add_argument(
        '--autocheck',
        required=False,
        action='store_true',
        help='Will automatically download all scans from XNAT '
             'that are not currently in the project folder.')
    parser.add_argument(
        '--xnat_experiment',
        required=False,
        dest='xnatexp',
        default=None,
        help='XNAT Experiment ID (i.e., XNAT_E*) for single '
             'session download.')
    return parser


def main(bids_dir, config, work_dir=None, protocol_check=False,
         autocheck=False, xnatexp=None):
    """Runtime for CIS processing."""
    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if work_dir is None:
        work_dir = CIS_DIR

    proj_dir = os.path.dirname(bids_dir)
    if not op.isdir(proj_dir):
        raise ValueError('Project directory must be an existing directory!')

    if not op.isfile(config):
        raise ValueError('Argument "config" must be an existing file.')

    with open(config, 'r') as fo:
        config_options = json.load(fo)

    if 'project' not in config_options.keys():
        raise Exception('Config file must be updated with project field. '
                        'See sample config file for more information')

    proj_work_dir = op.join(work_dir, config_options['project'])
    if not proj_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    xnatdownload_file = op.join('/home/data/cis/singularity-images/',
                                config_options['xnatdownload'])

    # Additional checks and copying for XNAT Download file
    if not op.isfile(xnatdownload_file):
        raise ValueError('XNAT Download image specified in config files must '
                         'be an existing file.')

    # Make folders/files

    for out_file in ['err', 'out']:
        if not op.isdir(op.join(proj_dir, 'code', out_file)):
            os.makedirs(op.join(proj_dir, 'code', out_file))

    if not op.isdir(proj_work_dir):
        os.makedirs(proj_work_dir)

    raw_dir = op.join(proj_dir, 'raw')
    if not op.isdir(raw_dir):
        os.makedirs(raw_dir)

    fdir = op.dirname(__file__)

    scans_df = pd.read_csv(op.join(raw_dir, 'scans.tsv'), sep='\t')
    scans_df = scans_df['file']
    scans_df.to_csv(
        op.join(
            proj_work_dir,
            '{0}-processed.txt'.format(config_options['project'])),
        sep='\t', line_terminator='\n', na_rep='n/a', index=False)

    # Copy singularity images to scratch
    scratch_xnatdownload = op.join(work_dir, op.basename(xnatdownload_file))
    if not op.isfile(scratch_xnatdownload):
        shutil.copyfile(xnatdownload_file, scratch_xnatdownload)
        os.chmod(scratch_xnatdownload, 0o775)

    # Run XNAT Download
    if autocheck:
        tar_list = op.join(
            proj_work_dir,
            '{0}-processed.txt'.format(config_options['project']))
        cmd = ('{sing} -w {work_dir} --project {proj} --autocheck --processed '
               '{tar_list}'.format(
                   sing=scratch_xnatdownload,
                   work_dir=proj_work_dir,
                   proj=config_options['project'],
                   tar_list=tar_list))
        run(cmd)
    elif xnatexp is not None:
        tar_list = op.join(
            proj_work_dir,
            '{0}-processed.txt'.format(config_options['project']))
        cmd = ('{sing} -w {work_dir} --project {proj} --session {xnat_exp} '
               '--processed {tar_list}'.format(
                   sing=scratch_xnatdownload,
                   work_dir=proj_work_dir,
                   proj=config_options['project'],
                   xnat_exp=xnatexp,
                   tar_list=tar_list))
        run(cmd)
    else:
        raise Exception('A valid XNAT Experiment session was not entered for '
                        'the project or you are not running autocheck.')

    os.remove(
        op.join(
            proj_work_dir,
            '{0}-processed.txt'.format(config_options['project'])))
    os.remove(scratch_xnatdownload)
    # Temporary raw directory in work_dir
    raw_work_dir = op.join(proj_work_dir, 'raw')

    if op.isdir(raw_work_dir):
        # Check if anything was downloaded
        for tmp_sub in os.listdir(raw_work_dir):
            ses_list = os.listdir(op.join(raw_work_dir, tmp_sub))
            for tmp_ses in ses_list:
                # run the protocol check if requested
                if protocol_check:
                    cmd = ('python {fdir}/protocol_check.py -w {work_dir} '
                           '--bids_dir {bids_dir} '
                           '--sub {sub} --ses {ses}'.format(
                               fdir=fdir,
                               work_dir=raw_work_dir,
                               bids_dir=bids_dir,
                               sub=tmp_sub,
                               ses=tmp_ses))
                    run(cmd)

                # tar the subject and session directory and copy to raw dir
                if not op.isdir(op.join(raw_dir, tmp_sub, tmp_ses)):
                    os.makedirs(op.join(raw_dir, tmp_sub, tmp_ses))

                tarball = op.join(
                    raw_dir,
                    '{sub}/{ses}/{sub}-{ses}.tar'.format(
                        sub=tmp_sub, ses=tmp_ses)
                )
                with tarfile.open(tarball, 'w') as tar:
                    tar.add(
                        op.join(raw_work_dir, tmp_sub),
                        arcname=op.basename(op.join(raw_work_dir, tmp_sub)))
                    shutil.rmtree(op.join(raw_work_dir, tmp_sub))

                scans_df = pd.read_csv(op.join(raw_dir, 'scans.tsv'), sep='\t')
                tmp_df = pd.DataFrame()
                tmp_df = tmp_df.append({'sub': tmp_sub}, ignore_index=True)
                tmp_df['ses'] = tmp_ses
                tmp_df['file'] = '{sub}-{ses}.tar'.format(
                    sub=tmp_sub, ses=tmp_ses)
                moddate = os.path.getmtime(tarball)
                timedateobj = datetime.datetime.fromtimestamp(moddate)
                tmp_df['creation'] = datetime.datetime.strftime(
                    timedateobj, "%m/%d/%Y, %H:%M")
                scans_df = scans_df.append(tmp_df)
                scans_df.to_csv(
                    op.join(raw_dir, 'scans.tsv'), sep='\t',
                    line_terminator='\n', na_rep='n/a', index=False)

                # run conversion_workflow.py
                err_file = op.join(
                    proj_dir,
                    'code/err/convert-{0}-{1}'.format(tmp_sub, tmp_ses)
                )
                out_file = op.join(
                    proj_dir,
                    'code/out/convert-{0}-{1}'.format(tmp_sub, tmp_ses)
                )
                cmd = ('sbatch -J convert-{proj}-{sub}-{ses} '
                       '-e {err_file_loc} -o {out_file_loc} '
                       '-c {nprocs} --qos {hpc_queue} --account {hpc_acct} '
                       '-p centos7 '
                       '--wrap="python {fdir}/conversion_workflow.py -t {tarball} '
                       '-b {bids_dir} -w {work_dir} --config {config} '
                       '--sub {sub} --ses {ses}"'.format(
                           fdir=fdir,
                           hpc_queue=config_options['hpc_queue'],
                           hpc_acct=config_options['hpc_account'],
                           proj=config_options['project'],
                           err_file_loc=err_file,
                           out_file_loc=out_file,
                           tarball=tarball,
                           bids_dir=bids_dir,
                           work_dir=proj_work_dir,
                           config=config,
                           sub=tmp_sub.strip('sub-'),
                           ses=tmp_ses.strip('ses-')))
                run(cmd)

                # get date and time
                now = datetime.datetime.now()
                date_time = now.strftime("%Y-%m-%d %H:%M")

                # append the email message
                message_file = op.join(
                    proj_work_dir,
                    '{0}-processed-message.txt'.format(
                        config_options['project']))
                with open(message_file, 'a') as fo:
                    fo.write('Data transferred from XNAT to FIU-HPC for '
                             'Project: {proj} Subject: {sub} Session: {ses} '
                             'on {datetime}\n'.format(
                                 proj=config_options['project'],
                                 sub=tmp_sub,
                                 ses=tmp_ses,
                                 datetime=date_time))

        shutil.rmtree(op.join(raw_work_dir))

        cmd = ("mail -s 'FIU XNAT-HPC Data Transfer Update Project {proj}' "
               "{email_list} < {message}".format(
                   proj=config_options['project'],
                   email_list=config_options['email'],
                   message=message_file))
        run(cmd)
        os.remove(message_file)


def _main(argv=None):
    options = _get_parser().parse_args(argv)
    kwargs = vars(options)
    main(**kwargs)


if __name__ == '__main__':
    _main()
