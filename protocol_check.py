"""Run a protocol check."""
import os
import os.path as op
import json
import argparse


def _get_parser():
    parser = argparse.ArgumentParser(
        description='Check scans for project protocol compliance.')
    parser.add_argument('-w', '--workdir', required=True, dest='work_dir',
                        help='Path to a working directory.')
    parser.add_argument('--bids_dir', required=True, dest='bids_dir',
                        help='Full path to the BIDS directory.')
    parser.add_argument('--sub', required=True, dest='sub',
                        help='The label of the subject to analyze.')
    parser.add_argument('--ses', required=True, dest='ses',
                        help='Session number', default=None)
    return parser


def main(work_dir, bids_dir, sub, ses=None):
    # Check inputs
    if not op.isdir(work_dir):
        raise ValueError('Argument "workdir" must be an existing directory.')

    if not op.isdir(op.dirname(bids_dir)):
        raise ValueError('Argument "bids_dir" must be an existing directory.')

    config_file = op.join(op.dirname(bids_dir), 'code/config.json')
    message_file = op.join(
        work_dir, '{sub}-{ses}-protocol_error.txt'.format(sub=sub, ses=ses))

    with open(config_file, 'r') as fo:
        config_options = json.load(fo)
    protocol_file = op.join(op.dirname(bids_dir), config_options['protocol'])

    if not op.isfile(protocol_file):
        raise ValueError('Argument "protocol" must exist.')

    # Additional checks
    if not op.isdir(op.join(work_dir, sub)):
        raise ValueError('Subject directory does not exist '
                         'in working directory.')

    if not op.isdir(op.join(work_dir, sub, ses)):
        raise ValueError('Session directory does not exist in subjects '
                         'working directory.')

    with open(protocol_file, 'r') as fo:
        protocol_options = json.load(fo)

    warning = False
    scans = protocol_options.keys()
    scan_list = os.listdir(op.join(work_dir, sub, ses))
    for tmp_scan in scans:
        if (tmp_scan != 'email') and (tmp_scan != 'project'):
            num = protocol_options[tmp_scan]['num']
            n_dicoms_required = protocol_options[tmp_scan]['dicoms']

            ignore_names = ['PMU', 'setter']
            tmp_scan_list = []
            for tmp in scan_list:
                if ((tmp_scan in tmp)
                   and all([igname not in tmp for igname in ignore_names])):
                    tmp_scan_list.append(tmp)

            if len(tmp_scan_list) != num:
                warning = True
                with open(message_file, 'a') as fo:
                    fo.write('There are {0} scans for {1}, but should be {2}'
                             '\n'.format(len(tmp_scan_list), tmp_scan, num))

            for t in tmp_scan_list:
                dicom_dir = op.join(
                    work_dir, sub, ses, t, 'resources/DICOM/files')
                n_dicoms_found = len(os.listdir(dicom_dir))
                if n_dicoms_found != n_dicoms_required:
                    warning = True
                    with open(message_file, 'a') as fo:
                        fo.write('There are {0} DICOMs for {1}, but should be '
                                 '{2}\n'.format(n_dicoms_found,
                                                t, n_dicoms_required))

    if warning:
        cmd = ("mail -s '{proj} Protocol Check Warning {sub} {ses}' "
               "{email_list} < {message}".format(
                   proj=protocol_options['project'],
                   sub=sub,
                   ses=ses,
                   email_list=protocol_options['email'],
                   message=message_file))
        os.system(cmd)
        os.remove(message_file)


def _main(argv=None):
    options = _get_parser().parse_args(argv)
    kwargs = vars(options)
    main(**kwargs)


if __name__ == '__main__':
    _main()
