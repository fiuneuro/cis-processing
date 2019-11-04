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
import datetime
import getpass

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
        line=str(line, 'utf-8')[:-1]
        print(line)
        if line == '' and process.poll() is not None:
            break

    if process.returncode != 0:
        raise Exception("Non zero return code: {0}\n"
                        "{1}\n\n{2}".format(process.returncode, command,
                                            process.stdout.read()))


def get_parser():
    parser = argparse.ArgumentParser(description='Initiate XNAT download and CIS proc.')
    parser.add_argument('-b', '--bidsdir', required=True, dest='bids_dir',
                        help=('Output directory for BIDS dataset and '
                              'derivatives.'))
    parser.add_argument('-w', '--workdir', required=False, dest='work_dir',
                        default=None,
                        help='Path to a working directory. Defaults to work '
                             'subfolder in dset_dir.')
    parser.add_argument('--config', required=True, dest='config',
                        help='Path to the config json file.')
    parser.add_argument('--protocol_check', required=False, action='store_true',
                        help='Will perform a protocol check to determine if the correct number of scans and TRs are present.')
    parser.add_argument('--autocheck', required=False, action='store_true',
                        help='Will automatically download all scans from XNAT that are not currently in the project folder.')
    parser.add_argument('--xnat_experiment', required=False, dest='xnatexp',
                        default=None,
                        help='XNAT Experiment ID (i.e., XNAT_E*) for single session download.')
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

    proj_dir = os.path.dirname(args.bids_dir)
    if not op.isdir(proj_dir):
         raise ValueError('Project directory must be an existing directory!')
        
    if not op.isfile(args.config):
        raise ValueError('Argument "config" must be an existing file.')

    if args.n_procs < 1:
        raise ValueError('Argument "n_procs" must be positive integer greater '
                         'than zero.')
    else:
        n_procs = int(args.n_procs)

    with open(args.config, 'r') as fo:
        config_options = json.load(fo)

    if 'project' not in config_options.keys():
        raise Exception('Config File must be updated with project field'
                        'See Sample Config File for More information')
                        
    proj_work_dir = op.join(args.work_dir,
                                '{0}'.format(config_options['project']))
    if not proj_work_dir.startswith('/scratch'):
        raise ValueError('Working directory must be in scratch.')

    xnatdownload_file = op.join('/home/data/cis/singularity-images/',
                         config_options['xnatdownload'])

    # Additional checks and copying for XNAT Download file
    if not op.isfile(xnatdownload_file):
        raise ValueError('XNAT Download image specified in config files must be '
                         'an existing file.')

        
    # Make folders/files
    if not op.isdir(op.join(proj_dir, 'code/err')):
        os.makedirs(op.join(proj_dir, 'code/err'))
        
    if not op.isdir(op.join(proj_dir, 'code/out')):
        os.makedirs(op.join(proj_dir, 'code/out'))
    
    if not op.isdir(proj_work_dir):
        os.makedirs(proj_work_dir)
    
    raw_dir = op.join(proj_dir, 'raw')
    if not op.isdir(raw_dir):
        os.makedirs(raw_dir)
        
    fdir = op.dirname(__file__)
    
    scans_df = pd.read_csv(op.join(raw_dir, 'scans.tsv'), sep='\t')
    scans_df = scans_df['file']
    scans_df.to_csv(op.join(proj_work_dir, '{0}-processed.txt'.format(config_options['project'])), index=False)

    # Copy singularity images to scratch
    scratch_xnatdownload = op.join(args.work_dir, op.basename(xnatdownload_file))
    if not op.isfile(scratch_xnatdownload):
        shutil.copyfile(xnatdownload_file, scratch_xnatdownload)
        os.chmod(scratch_xnatdownload, 0o775)

    # Run XNAT Download
    if args.autocheck:
        cmd = ('{sing} -w {work} --project {proj} --autocheck --processed {tar_list}'.format(sing=scratch_xnatdownload, work=proj_work_dir, proj=config_options['project'], tar_list=op.join(proj_work_dir, '{0}-processed.txt'.format(config_options['project']))))
        run(cmd)
    elif args.xnatexp is not None:
        cmd = ('{sing} -w {work} --project {proj} --session {xnat_exp} --processed {tar_list}'.format(sing=scratch_xnatdownload, work=proj_work_dir, proj=config_options['project'], xnat_exp=args.xnatexp, tar_list=op.join(proj_work_dir, '{0}-processed.txt'.format(config_options['project']))))
        run(cmd)
    else:
        raise Exception('A valid XNAT Experiment session was not entered for the project or you are not running autocheck.')
    
    os.remove(op.join(proj_work_dir, '{0}-processed.txt'.format(config_options['project'])))
    os.remove(scratch_xnatdownload)
    # Temporary raw directory in work_dir
    raw_work_dir = op.join(proj_work_dir, 'raw')
    
    if op.isdir(raw_work_dir):
        # Check if anything was downloaded
        data_download = False
        if os.listdir(raw_work_dir):
            data_download = True

        if data_download:
            
            sub_list = os.listdir(raw_work_dir)
            for tmp_sub in sub_list:
                ses_list = os.listdir(op.join(raw_work_dir, '{0}'.format(tmp_sub)))
                
                for tmp_ses in ses_list:
                    #run the protocol check if requested
                    if args.protocol_check:
                        cmd = ('python {fdir}/protocol_check.py -w {work} --bids_dir {bids_dir} --sub {sub} --ses {ses}'.format(fdir=fdir, work=raw_work_dir, bids_dir = args.bids_dir, sub=tmp_sub, ses=tmp_ses))
                        run(cmd)
                    
                    #tar the subject and session directory and copy to raw dir
                    if not op.isdir(op.join(raw_dir, tmp_sub, tmp_ses)):
                        os.makedirs(op.join(raw_dir, tmp_sub, tmp_ses))
                    with tarfile.open(op.join(raw_dir, tmp_sub, tmp_ses, '{sub}-{ses}.tar'.format(sub=tmp_sub, ses=tmp_ses)), "w") as tar:
                        tar.add(op.join(raw_work_dir, '{sub}'.format(sub=tmp_sub)), arcname=os.path.basename(op.join(raw_work_dir, '{sub}'.format(sub=tmp_sub))))
                        shutil.rmtree(op.join(raw_work_dir, '{sub}'.format(sub=tmp_sub)))
                    
                    scans_df = pd.read_csv(op.join(raw_dir, 'scans.tsv'), sep='\t')
                    cols = scans_df.columns
                    tmp_df = pd.DataFrame()
                    tmp_df = tmp_df.append({'sub': tmp_sub}, ignore_index=True)
                    tmp_df['ses'] = tmp_ses
                    tmp_df['file'] = '{sub}-{ses}.tar'.format(sub=tmp_sub, ses=tmp_ses)
                    moddate = os.path.getmtime(op.join(raw_dir, tmp_sub, tmp_ses, '{sub}-{ses}.tar'.format(sub=tmp_sub, ses=tmp_ses)))
                    timedateobj = datetime.datetime.fromtimestamp(moddate)
                    tmp_df['creation'] = datetime.datetime.strftime(timedateobj, "%m/%d/%Y, %H:%M")
                    scans_df = scans_df.append(tmp_df)
                    scans_df.to_csv(op.join(raw_dir, 'scans.tsv'), sep='\t', index=False)
                                               
                    # run cis_proc.py
                    cmd = ('sbatch -J cis_proc-{proj}-{sub}-{ses} -e {err_file_loc} -o {out_file_loc} -c {nprocs} --qos {hpc_queue} --account {hpc_acct} -p centos7 --wrap="python {fdir}/cis_proc.py -t {tarfile} -b {bidsdir} -w {work} --config {config} --sub {sub} --ses {ses} --n_procs {nprocs}"'. format(fdir=fdir, hpc_queue=config_options['hpc_queue'], hpc_acct=config_options['hpc_account'], proj=config_options['project'], err_file_loc = op.join(proj_dir, 'code/err', 'cis_proc-{sub}-{ses}'.format(sub=tmp_sub, ses=tmp_ses)), out_file_loc= op.join(proj_dir, 'code/out', 'cis_proc-{sub}-{ses}'.format(sub=tmp_sub, ses=tmp_ses)), tarfile=op.join(raw_dir, tmp_sub, tmp_ses, '{sub}-{ses}.tar'.format(sub=tmp_sub, ses=tmp_ses)), bidsdir=args.bids_dir, work=proj_work_dir, config=args.config, sub=tmp_sub.strip('sub-'), ses=tmp_ses.strip('ses-'), nprocs=args.n_procs))
                    run(cmd)

                    # get date and time
                    now = datetime.datetime.now()
                    date_time=now.strftime("%Y-%m-%d %H:%M")
                    
                    # append the email message
                    
                    with open(op.join(proj_work_dir, '{0}-processed-message.txt'.format(config_options['project'])), 'a') as fo:
                        fo.write('Data transferred from XNAT to FIU-HPC for Project: {proj} Subject: {sub} Session: {ses} on {datetime}\n'.format(proj=config_options['project'], sub=tmp_sub, ses=tmp_ses, datetime=date_time))
                    
            
            shutil.rmtree(op.join(raw_work_dir))

            cmd = ("mail -s 'FIU XNAT-HPC Data Transfer Update Project {proj}' {email_list} < {message}".format(proj=config_options['project'], email_list=config_options['email'], message=op.join(proj_work_dir, '{0}-processed-message.txt'.format(config_options['project']))))
            run(cmd)
            os.remove(op.join(proj_work_dir, '{0}-processed-message.txt'.format(config_options['project'])))

if __name__ == '__main__':
    main()
