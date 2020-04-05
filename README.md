# cis-processing
Automated transfer, conversion, and processing of data acquired at the FIU
Center for Imaging Science.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/FIU-Neuro/cis-processing.svg?style=shield)](https://circleci.com/gh/FIU-Neuro/cis-processing/tree/master)

## Why we built this tool
We wanted to make it easier for researchers in our department to convert their
neuroimaging data to BIDS format and to perform quality control on said data.
To that end, we download incoming data from XNAT and automatically
convert/process the data with a set of Singularity images.
There are a handful of criteria that we wanted to meet which
meant that out-of-the-box tools (e.g., `heudiconv`) would not be sufficient on
their own. Here is a list, in no particular order:
  - Conversion/processing code should be static across the course
    of a single study, but will vary between studies, necessitating the use of
    Singularity images.
  - Data must be stored long-term in the `/data` directory, but
    all files must be copied to `/scratch` for processing. While processing nodes
    used by the SLURM scheduler can access both `/scratch` and `/data`,
    Singularity images can only access `/scratch`, so we need a wrapper script
    will copy files to `/scratch` before conversion and then copy results back
    to `/data` after the job has finished.
  - MRIQC IQMs across projects are compiled into a single database to track scan
    quality at the FIU MRI over time and to identify scanner-specific outliers in
    IQMs in order to flag bad runs/subjects.
  - Pipeline should be relatively simple to use for researchers in the
    department, especially those new to BIDS.
  - Pipeline should be called automatically, but separately, by each PI, as
    there is currently no budget for centralized CIS processing.
  - Datasets should be anonymized (including removal of PHI from metadata and
    defacing of anatomical scans) to make it easier to share data later on.

## Usage
If you would like to use the cis-processing pipeline, you'll first need to do a couple of things:
1. Create a [heudiconv](https://github.com/nipy/heudiconv) heuristic file for your project.
    - This file specifies how the dicom converter will select, convert, and rename scans to match BIDS format. In order to avoid converting incomplete or incorrect scans, the heuristic file allows you to check things like the number of slices, the number of volumes, and the name for each scan.
    - Heuristic files for several projects are included in this repository, in the [heuristics](https://github.com/FIU-Neuro/cis-processing/tree/master/heuristics) folder. You do not need to upload your heuristic file to the repository, although if you don't then that will need to be reflected in the project's config file.
2. Create a config file for your project.
    - This file specifies a number of important things, including:
        - The location and name of your heuristic file. **If you don't want to upload your heuristic file to this repository, make sure to include the full path to the heuristic file in the config file.**
        - The BIDSification and MRIQC Singularity images you want to use for your project.
        - Any project-specific parameters you might want to specify for MRIQC (esp. the FD threshold you use to identify motion outliers).
    - The config file **does not** need to be uploaded to this repository. The file is specified in the call to `run.py`.
3. Optional: Upload your config and heuristic files to this repository.
    - You can open a pull request with the uploaded files from your fork to this repository, and one of the maintainers of the repository will review and merge your changes.
4. Create a job file for the HPC.
    - You can use the provided [template job](https://github.com/FIU-Neuro/cis-processing/blob/master/example_slurm_job.sh) as a basis for your own.
    - Remember that the DICOM tar file input (`-t` or `--tarfile`) should *just* contain the scan-specific folders to be converted (e.g., `1-localizer`, `2-MPRAGE`, etc.). This generally comes from a folder called `scans/` and is subject- and session-specific.
5. Submit your job.

## Support
If you identify a bug, need help getting started, or would like to request new features, please first check that a similar issue does not already exist. If one doesn't, please feel free to [open an issue](https://github.com/FIU-Neuro/cis-processing/issues) in this repository detailing your error/question/request and the project maintainers will attempt to help.

## Relationship to other DICOM conversion tools
- [cis-xget](https://github.com/FIU-Neuro/cis-xget):
  The cis-xget package acts as a sub-workflow for `cis-processing`.
  cis-xget downloads new scans from XNAT automatically.
  Because it requires additional dependencies, cis-xget is built as a
  Singularity image, which in turn is called within cis-processing.
- [cis-bidsify](https://github.com/FIU-Neuro/cis-bidsify):
  This package acts as an additional sub-workflow for `cis-processing`.
  cis-bidsify uses heudiconv and pybids to convert the DICOMs downloaded by
  `cis-xget` into BIDS format, and then to perform some light anonymization and
  metadata completion.
  This sub-workflow is built as a Singularity image and is called within
  cis-processing.
- [Heudiconv](https://heudiconv.readthedocs.io):
  We use heudiconv to perform the conversion from raw DICOMs to BIDS dataset.
  Users must provide a valid heudiconv heuristic file to `cis-processing`.
- [ReproIn](https://github.com/ReproNim/reproin):
  ReproIn serves as a standard for organizing and labeling your protocols at
  the scanner. By following this standard at acquisition, you can use the
  ReproIn heudiconv heuristic instead of designing one yourself.
  ReproIn-compatible protocols will also be easier for other researchers to
  understand and copy, as they follow a public standard.

## Other BIDSification options
There are a number of BIDS converter tools.
For a more comprehensive list, see [here](https://bids.neuroimaging.io/benefits.html#converters).

- [BIDScoin](https://bidscoin.readthedocs.io)
- [dcmwrangle](https://github.com/jbteves/dcmwrangle)
