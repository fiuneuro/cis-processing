"""
BIDS version: 1.0.1
"""
import os


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
    boldt1 = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-nback_run-{item:02d}_bold',
                        outtype=outtype)
    boldt2 = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-mid_run-{item:02d}_bold',
                        outtype=outtype)
    rest = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-rest_run-{item:02d}_bold',
                        outtype=outtype)

    # field maps
    fmap_rest = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-rest_dir-{dir}_run-{item:02d}_epi',
                           outtype=outtype)
    fmap_t1 = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-nback_dir-{dir}_run-{item:02d}_epi',
                         outtype=outtype)
    fmap_t2 = create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-mid_dir-{dir}_run-{item:02d}_epi',
                         outtype=outtype)

    # structurals
    t1 = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1w',
                    outtype=outtype)

    info = {boldt1: [], boldt2: [], rest: [],
            fmap_rest: [], fmap_t1: [], fmap_t2: [],
            t1: []}
    last_run = len(seqinfo)
    for i, s in enumerate(seqinfo):
        x, y, sl, nt = (s[6], s[7], s[8], s[9])
        if ((sl == 176) and (s[12].endswith('T1w_MPR_vNav'))):
            next_scan = seqinfo[i+1]
            if next_scan[12].endswith('T1w_MPR_vNav'):
                pass
            else:
                info[t1].append([s[2]])
        elif ((s[12].endswith('fMRI_task_n-back_1')) or (s[12].endswith('fMRI_task_n-back_2')) or (s[12].endswith('fMRI_task_n-back_3'))):
            info[boldt1].append(s[2])
        elif ((s[12].endswith('fMRI_task_MID1')) or (s[12].endswith('fMRI_task_MID2')) or (s[12].endswith('fMRI_task_MID3')) or (s[12].endswith('fMRI_task_MID4'))):
            info[boldt2].append(s[2])
        elif s[12].endswith('fMRI_rest'):
            info[rest].append(s[2])
        elif 'DistortionMap' in s[12]:
            if i < last_run-2:

                if s[12].endswith('DistortionMap_PA'):
                    next_scan = seqinfo[i+2]
                    dir_ = 'PA'
                elif s[12].endswith('DistortionMap_AP'):
                    next_scan = seqinfo[i+1]
                    dir_ = 'AP'

                if (next_scan[12].endswith('fMRI_rest')):
                    info[fmap_rest].append({'item': s[2], 'dir': dir_,
                                            'acq': 'rest'})
                elif ((next_scan[12].endswith('fMRI_task_n-back_1')) or (next_scan[12].endswith('fMRI_task_n-back_2')) or (next_scan[12].endswith('fMRI_task_n-back_3'))):
                    info[fmap_t1].append({'item': s[2], 'dir': dir_,
                                          'acq': 'nback'})
                elif ((next_scan[12].endswith('fMRI_task_MID1')) or (next_scan[12].endswith('fMRI_task_MID2')) or (next_scan[12].endswith('fMRI_task_MID3')) or (next_scan[12].endswith('fMRI_task_MID4'))):
                    info[fmap_t2].append({'item': s[2], 'dir': dir_,
                                          'acq': 'mid'})
        else:
            pass
    return info
