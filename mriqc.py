#!/usr/bin/env python3
"""
Compile group-level MRIQC results.
"""
import os
import os.path as op
import re
import json
import shutil
import datetime
from glob import glob

import pandas as pd

from utils import run


def run_mriqc(bids_dir, templateflow_dir, mriqc_singularity_image, work_dir,
              out_dir, config_options, sub=None, ses=None, n_procs=1):
    """Run MRIQC"""
    # Run MRIQC anat
    mriqc_anat_modality = config_options['mriqc_options']['anat']['mod']
    for tmp_mod in mriqc_anat_modality.keys():
        kwarg_str = ''
        settings_dict = mriqc_anat_modality[tmp_mod]['mriqc_settings']
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
                   templateflowdir=templateflow_dir,
                   sing=mriqc_singularity_image,
                   bids=bids_dir,
                   out=out_dir,
                   mod=tmp_mod,
                   work=work_dir,
                   n_procs=n_procs,
                   kwarg_str=kwarg_str))
        run(cmd)

    # Run MRIQC func
    mriqc_tasks = config_options['mriqc_options']['func']['task']
    for tmp_task in mriqc_tasks.keys():
        run_mriqc = False
        task_json_fname = (
            'sub-{sub}/func/sub-{sub}_task-{task}_run-01_bold.'
            'json'.format(sub=sub, task=tmp_task))
        if ses:
            task_json_fname = (
                'sub-{sub}/ses-{ses}/func/sub-{sub}_ses-{ses}_'
                'task-{task}_run-01_bold.json'.format(
                    sub=sub, ses=ses, task=tmp_task))
        task_json_file = op.join(
            bids_dir,
            task_json_fname)
        if op.isfile(task_json_file):
            run_mriqc = True

        if run_mriqc:
            kwarg_str = ''
            settings_dict = mriqc_tasks[tmp_task]['mriqc_settings']
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
                       templateflowdir=templateflow_dir,
                       sing=mriqc_singularity_image,
                       bids=bids_dir,
                       out=out_dir,
                       task=tmp_task,
                       work=work_dir,
                       n_procs=n_procs,
                       kwarg_str=kwarg_str))
            run(cmd)


def merge_mriqc_derivatives(scratch_deriv_dir, out_deriv_dir):
    """Merge MRIQC results into final derivatives folder"""
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
            out_df.to_csv(out_file, line_terminator='\n', index=False)


def mriqc_group(bids_dir, config, work_dir=None, sub=None, ses=None,
                participant=False, group=False, n_procs=1):
    """I don't know what this does."""
    CIS_DIR = '/scratch/cis_dataqc/'

    # Check inputs
    if work_dir is None:
        work_dir = CIS_DIR

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

    if not work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    mriqc_file = op.join('/home/data/cis/singularity-images/',
                         config_options['mriqc'])
    mriqc_version = re.search(r'_([\d.]+)', mriqc_file).group(1)

    out_deriv_dir = op.join(bids_dir,
                            'derivatives/mriqc-{0}'.format(mriqc_version))

    out_dir = op.join(work_dir, 'mriqc')
    scratch_mriqc_work_dir = op.join(work_dir, 'mriqc-wkdir')

    if not op.isfile(mriqc_file):
        raise ValueError('MRIQC image specified in config files must be '
                         'an existing file.')

    # Copy singularity images to scratch
    scratch_mriqc = op.join(CIS_DIR, op.basename(mriqc_file))

    if not op.isfile(scratch_mriqc):
        shutil.copyfile(mriqc_file, scratch_mriqc)
        os.chmod(scratch_mriqc, 0o775)

    if group:
        shutil.copytree(out_deriv_dir, out_dir)
        cmd = ('{sing} {bids} {out} group --no-sub --verbose-reports '
               '-w {work} --n_procs {n_procs} '.format(
                   sing=scratch_mriqc, bids=bids_dir,
                   out=out_dir,
                   work=scratch_mriqc_work_dir, n_procs=n_procs))
        run(cmd)

    for modality in ['bold', 'T1w', 'T2w']:
        out_csv = op.join(out_dir, modality + '.csv')
        out_html = op.join(out_dir, 'reports', modality + '.html')
        if op.isfile(out_csv):
            shutil.copy(out_csv, out_deriv_dir)
            shutil.copy(out_html, op.join(out_deriv_dir, 'reports'))

    # get date and time
    now = datetime.datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M")
    # append the email message
    message_file = op.join(
        work_dir,
        '{0}-mriqc-message.txt'.format(config_options['project']))
    with open(message_file, 'a') as fo:
        fo.write('Group quality control report for {proj} prepared on '
                 '{datetime}\n'.format(
                     proj=config_options['project'],
                     datetime=date_time))

    cmd = ("mail -s '{proj} MRIQC Group Report' "
           "-a {mriqc_dir}/reports/bold_group.html "
           "-a {mriqc_dir}/reports/T1w_group.html {emails} < {message}".format(
               proj=config_options['project'],
               mriqc_dir=out_deriv_dir,
               emails=config_options['email'],
               message=message_file))
    run(cmd)

    shutil.rmtree(out_dir)
    os.remove(message_file)
