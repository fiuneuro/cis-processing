"""
BIDS version: 1.0.1
"""


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')
    return template, outtype, annotation_classes


def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where

    allowed template fields - follow python string module:

    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """

    outtype = ('nii.gz')
    # functionals
    rs = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-rest_run-{item:02d}_bold',
                    outtype=outtype)
    boldt1 = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-stopsignal_run-{item:02d}_bold',
                        outtype=outtype)
    boldt2 = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-emotionalnback_run-{item:02d}_bold',
                        outtype=outtype)
    boldt3 = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-monetaryincentive_run-{item:02d}_bold',
                        outtype=outtype)

    # dwi
    dwi = create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_run-{item:02d}_dwi',
                     outtype=outtype)

    # field maps
    fmap_func = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-rest_dir-{dir}_run-{item:02d}_epi',
                           outtype=outtype)
    fmap_dwi = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-dwi_dir-{dir}_run-{item:02d}_epi',
                          outtype=outtype)

    # structurals
    t1 = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_T1w',
                    outtype=outtype)
    t2 = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_T2w',
                    outtype=outtype)

    info = {rs: [], boldt1: [], boldt2: [], boldt3: [], dwi: [],
            fmap_func: [], fmap_dwi: [], t1: [], t2: []}
    last_run = len(seqinfo)
    for i, s in enumerate(seqinfo):
        x, y, sl, nt = (s[6], s[7], s[8], s[9])
        if (sl == 176) and (s[12].endswith('ABCD_T1w_MPR_vNav')):
            info[t1] = [s[2]]
        elif (sl == 176) and (s[12].endswith('ABCD_T2w_SPC_vNav')):
            info[t2] = [s[2]]
        elif (nt == 383) and (s[12].endswith('ABCD_fMRI_rest')):
            info[rs].append(s[2])
        elif (nt == 445) and (s[12].endswith('ABCD_fMRI_task_Stop')):
            info[boldt1].append(s[2])
        elif (nt == 370) and (s[12].endswith('ABCD_fMRI_task_Emotional_n-back')):
            info[boldt2].append(s[2])
        elif (nt == 411) and (s[12].endswith('ABCD_fMRI_task_Monetary_Incentive')):
            info[boldt3].append(s[2])
        elif (sl == 81) and (nt == 103) and (s[12].endswith('ABCD_dMRI')):
            info[dwi].append(s[2])
        elif 'DistortionMap' in s[12]:
            # ABCD field maps should follow BIDS format 4 (2 phase maps)
            if i < last_run-1:
                next_scan = seqinfo[i+1]
                if i < last_run - 2:
                    next_next_scan = seqinfo[i+2]
                else:
                    next_next_scan = seqinfo[i+1]  # dupe of next_scan

                if s[12].endswith('DistortionMap_PA'):
                    dir_ = 'PA'
                elif s[12].endswith('DistortionMap_AP'):
                    dir_ = 'AP'
                else:
                    raise ValueError('Fieldmap scan {0} not '
                                     'supported'.format(s[12]))

                if (next_scan[12].endswith('dMRI') or next_next_scan[12].endswith('dMRI')) \
                        and 'fMRI' not in next_scan[12]:
                    info[fmap_dwi].append({'item': s[2], 'dir': dir_,
                                           'acq': 'dwi'})
                else:
                    info[fmap_func].append({'item': s[2], 'dir': dir_,
                                            'acq': 'func'})
        else:
            pass
    return info
