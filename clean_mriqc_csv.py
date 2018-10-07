"""
Convert NaNs in a csv to zeros. Necessary for using the MRIQC
classifier on datasets which may have skull-stripped data.
This problem is discussed here (with no solution):
https://github.com/poldracklab/mriqc/issues/546

Usage: python clean_csv.py [file]
"""

import sys
from os.path import dirname, basename, splitext, join
import pandas as pd


def main(in_file):
    fname = basename(in_file)
    d = dirname(in_file)
    fname, ext = splitext(fname)
    out_fname = fname + '_cleaned'
    out_file = join(d, out_fname+ext)

    df = pd.read_csv(in_file)
    df = df.fillna(0)
    df.to_csv(out_file, index=False)


if __name__ == "__main__":
    f = sys.argv[1]
    main(f)
