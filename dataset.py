import os.path as op
import shutil

import pandas as pd


def merge_datasets(scratch_bids_dir, bids_dir, project_name, sub, ses=None):
    """Merge BIDS dataset into final folder"""
    dset_files = ['CHANGES', 'README', 'dataset_description.json',
                  'participants.tsv']
    for dset_file in dset_files:
        if not op.isfile(op.join(bids_dir, dset_file)):
            shutil.copyfile(op.join(scratch_bids_dir, dset_file),
                            op.join(bids_dir, dset_file))

    new_participants_df = pd.read_csv(
        op.join(scratch_bids_dir, 'participants.tsv'),
        sep='\t').T.drop_duplicates().T
    orig_participants_df = pd.read_csv(
        op.join(bids_dir, 'participants.tsv'),
        sep='\t').T.drop_duplicates().T

    # Check if row already in participants file
    matches = new_participants_df[
        (new_participants_df == orig_participants_df.loc[0]).all(axis=1)
    ]
    match = matches.index.values.size
    if not match:
        new_participants_df = pd.concat(
            [new_participants_df, orig_participants_df])
        new_participants_df.to_csv(
            op.join(bids_dir, 'participants.tsv'),
            sep='\t', line_terminator='\n', index=False)
    else:
        print('Subject/session already found in participants.tsv')

    scratch_sub_dir = op.join(scratch_bids_dir, 'sub-{0}'.format(sub))
    out_sub_dir = op.join(bids_dir, 'sub-{0}'.format(sub))
    if not op.isdir(out_sub_dir):
        shutil.copytree(scratch_sub_dir, out_sub_dir)
    elif ses is not None:
        scratch_ses_dir = op.join(scratch_sub_dir, 'ses-{0}'.format(ses))
        out_ses_dir = op.join(out_sub_dir, 'ses-{0}'.format(ses))
        if not op.isdir(out_ses_dir):
            shutil.copytree(scratch_ses_dir, out_ses_dir)
        else:
            print('Warning: Subject/session directory '
                  'already exists in dataset.')
    else:
        print('Warning: Subject directory already exists in dataset.')

    scans_path = 'sub-{sub}_scans.tsv'.format(sub=sub)
    if ses:
        scans_path = 'ses-{ses}/sub-{sub}_ses-{ses}_scans.tsv'.format(
            sub=sub, ses=ses)
    sub_scans_df = pd.read_csv(
        op.join(out_sub_dir, scans_path),
        sep='\t')

    # append scans.tsv file with remove and annot fields
    sub_scans_df['remove'] = 0
    sub_scans_df['annotation'] = ''

    # import master scans file
    master_scans_file = op.join(
        op.dirname(bids_dir),
        'code/{}_scans.tsv'.format(project_name))
    if op.isfile(master_scans_file):
        master_scans_df = pd.read_csv(master_scans_file, sep='\t')
        master_df_headers = list(master_scans_df)
        master_scans_df = master_scans_df.append(sub_scans_df)
        master_scans_df.to_csv(master_scans_file, sep='\t',
                               line_terminator='\n', index=False,
                               columns=master_df_headers)
    else:
        tmp_df_headers = list(sub_scans_df)
        sub_scans_df.to_csv(
            master_scans_file, sep='\t', line_terminator='\n',
            index=False, columns=tmp_df_headers)
