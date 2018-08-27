"""
Anonymize acquisition datetimes for a dataset. Works for both longitudinal and
cross-sectional studies.
"""
import os.path as op

import pandas as pd
from dateutil import parser
from bids.grabbids import BIDSLayout


def anon_acqtimes(dset_dir):
    """
    Anonymize acquisition datetimes for a dataset. Works for both longitudinal
    and cross-sectional studies. The time of day is preserved, but the first
    scan is set to January 1st, 1800. In a longitudinal study, each session is
    anonymized relative to the first session, so that time between sessions is
    preserved.

    Overwrites scan tsv files in dataset. Only run this *after* data collection
    is complete for the study, especially if it's longitudinal.

    Parameters
    ----------
    dset_dir : str
        Path to BIDS dataset to be anonymized.
    """
    bl_dt = parser.parse('1800-01-01')

    layout = BIDSLayout(dset_dir)
    subjects = layout.get_subjects()
    sessions = sorted(layout.get_sessions())

    for sub in subjects:
        if not sessions:
            scans_file = op.join(dset_dir,
                                 'sub-{0}/sub-{0}_scans.tsv'.format(sub))
            df = pd.read_csv(scans_file, sep='\t')
            first_scan = df['acq_time'].min()
            first_dt = parser.parse(first_scan.split('T')[0])
            diff = first_dt - bl_dt
            acq_times = df['acq_time'].apply(parser.parse)
            acq_times = (acq_times - diff).astype(str)
            df['acq_time'] = acq_times
            # df.to_csv(scans_file, sep='\t', index=False)
        else:
            # Separated from dataset sessions in case subject missed some
            sub_ses = sorted(layout.get_sessions(subject=sub))
            for i, ses in enumerate(sub_ses):
                scans_file = op.join(dset_dir,
                                     'sub-{0}/ses-{1}/sub-{0}_ses-{1}_scans.'
                                     'tsv'.format(sub, ses))
                df = pd.read_csv(scans_file, sep='\t')
                if i == 0:
                    # Anonymize in terms of first scan for subject.
                    first_scan = df['acq_time'].min()
                    first_dt = parser.parse(first_scan.split('T')[0])
                    diff = first_dt - bl_dt

                acq_times = df['acq_time'].apply(parser.parse)
                acq_times = (acq_times - diff).astype(str)
                df['acq_time'] = acq_times
                # df.to_csv(scans_file, sep='\t', index=False)
