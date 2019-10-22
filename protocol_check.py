#!/usr/bin/env python3
"""
Based on
https://github.com/BIDS-Apps/example/blob/aa0d4808974d79c9fbe54d56d3b47bb2cf4e0a0d/run.py
"""
import os
import os.path as op
import json
import argparse


def get_parser():
    parser = argparse.ArgumentParser(description='Check scans for \
                                                  project protocol compliance.')
    parser.add_argument('-w', '--workdir', required=True, dest='work_dir',
                        help='Path to a working directory.')
    parser.add_argument('--bids_dir', required=True, dest='bids_dir',
                        help='Full path to the BIDS directory.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=True, dest='ses',
                        help='Session number', default=None)
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)

    # Check inputs
    if not os.path.isdir(args.work_dir):
        raise ValueError('Argument "workdir" must be an existing directory.')

    if not os.path.isdir(os.path.dirname(args.bids_dir)):
        raise ValueError('Argument "bids_dir" must be an existing directory.')

    with open(op.join(os.path.dirname(args.bids_dir),
                      'code/config.json'), 'r') as fo:
        config_options = json.load(fo)

    if not op.isfile(op.join(os.path.dirname(args.bids_dir),
                             config_options['protocol'])):
        raise ValueError('Argument "protocol" must exist.')

    # Additional checks
    if not os.path.isdir(op.join(args.work_dir, args.sub)):
        raise ValueError('Subject directory does \
                          not exist in working directory.')

    if not os.path.isdir(op.join(args.work_dir, args.sub, args.ses)):
        raise ValueError('Session directory does not exist \
                          in subjects working directory.')

    with open(op.join(os.path.dirname(args.bids_dir),
                      config_options['protocol']), 'r') as fo:
        protocol_options = json.load(fo)

    warning = False
    scans = protocol_options.keys()
    scan_list = os.listdir(op.join(args.work_dir, args.sub, args.ses))
    for tmp_scan in scans:
        if (tmp_scan != 'email') and (tmp_scan != 'project'):
            num = protocol_options[tmp_scan]['num']
            dicoms = protocol_options[tmp_scan]['dicoms']

            tmp_scan_list = [tmp for tmp in scan_list if ((tmp_scan in tmp) and (("PMU" not in tmp) and ("setter" not in tmp)))]

            if len(tmp_scan_list) != num:
                warning = True
                with open(op.join(args.work_dir,
                                  '{sub}-{ses}-protocol_error.txt'.format(sub=args.sub,
                                                                          ses=args.ses)),
                          'a') as fo:
                    fo.write('There are {0} scans for {1}, \
                              but should be {2} \n'.format(len(tmp_scan_list),
                                                           tmp_scan,
                                                           num))

            for t in tmp_scan_list:
                if len(os.listdir(op.join(args.work_dir,
                                          args.sub,
                                          args.ses,
                                          t,
                                          'resources/DICOM/files'))) != dicoms:
                    warning = True
                    with open(op.join(args.work_dir,
                                      '{sub}-{ses}-protocol_error.txt'.format(sub=args.sub,
                                                                              ses=args.ses)),
                              'a') as fo:
                        fo.write('There are {0} DICOMs for {1}, \
                                  but should be {2} \n'.format(len(os.listdir(op.join(args.work_dir,
                                                                                      args.sub,
                                                                                      args.ses,
                                                                                      t,
                                                                                      'resources/DICOM/files'))),
                                                               t,
                                                               dicoms))

    if warning:
        cmd = ("mail -s '{proj} Protocol Check Warning {sub} {ses}' {email_list} < {message}".format(proj=protocol_options['project'], sub=args.sub, ses=args.ses, email_list=protocol_options['email'], message=op.join(args.work_dir, '{sub}-{ses}-protocol_error.txt'.format(sub=args.sub, ses=args.ses))))
        os.system(cmd)
        os.remove(op.join(args.work_dir, '{sub}-{ses}-protocol_error.txt'.format(sub=args.sub, ses=args.ses)))


if __name__ == '__main__':
    main()
