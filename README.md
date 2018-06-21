# cis-processing
Automated transfer, conversion, and processing of data acquired at the FIU
Center for Imaging Science.

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
    used by the LSF scheduler can access both `/scratch` and `/data`,
    Singularity images can only access `/scratch`.
  - MRIQC IQMs across projects are compiled into a single database to track scan
    quality at the FIU MRI over time and to identify scanner-specific outliers in
    IQMs in order to flag bad runs/subjects.
  - Pipeline should be relatively simple to use for researchers in the
    department, especially those new to BIDS.
  - Pipeline should be called automatically, but separately, by each PI, as
    there is currently no budget for centralized CIS processing.
  - Datasets should be anonymized (including removal of PHI from metadata and
    defacing of anatomical scans) to make it easier to share data later on.
