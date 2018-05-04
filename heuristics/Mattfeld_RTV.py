import os


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')
    return (template, outtype, annotation_classes)


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
    rs = create_key('func/sub-{subject}_ses-{session}_task-rest_run-{item:02d}_bold',
                    outtype=outtype)
    boldt1 = create_key('func/sub-${subjects}_ses-${session}_task-rtv_run{item:02d}_bold',
                        outtype=outtype)

    # dwi
    dwi = create_key('dwi/sub-{subject}_ses-{session}_run-{item:02d}_dwi',
                     outtype=outtype)

    # field maps
    fmap_func = create_key('fmap/sub-{subject}_ses-{session}_acq-func_dir-{dir}_run-{item:02d}_epi',
                           outtype=outtype)
    fmap_dwi = create_key('fmap/sub-{subject}_ses-{session}_acq-dwi_dir-{dir}_run-{item:02d}_epi',
                          outtype=outtype)

    # structurals
    t1 = create_key('anat/sub-{subject}_ses-{session}_T1w',
                    outtype=outtype)

    info = {rs: [], boldt1: [], fmap_func: [], fmap_dwi: [], dwi: [], t1: []}
    last_run = len(seqinfo)
    for i, s in enumerate(seqinfo):
        x, y, sl, nt = (s[6], s[7], s[8], s[9])
        if (sl == 176) and s[12].endswith('T1w_MPR_vNav'):
            info[t1].append(s[2])
        elif (nt == 750) and ('fMRI_RTV_Rest' in s[12]):
            info[rs].append(int(s[2]))
        elif (nt == 380) and ('fMRI_RTV_Run' in s[12]):
            info[boldt1].append(s[2])
        elif (sl > 1) and (nt == 103) and ('dMRI' in s[12]):
            info[dwi].append(s[2])
        elif 'DistortionMap' in s[12]:
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
                elif s[12].endswith('DistortionMap_RL'):
                    dir_ = 'RL'
                elif s[12].endswith('DistortionMap_LR'):
                    dir_ = 'LR'
                else:
                    raise ValueError('Fieldmap scan {0} not '
                                     'supported'.format(s[12]))

                if (next_scan[12].endswith('dMRI') or next_next_scan[12].endswith('dMRI')) \
                        and 'fMRI' not in next_scan[12]:
                    info[fmap_dwi].append({'item': s[2], 'dir': dir_})
                else:
                    info[fmap_func].append({'item': s[2], 'dir': dir_})
        else:
            pass
    return info
