"""Utilities used by other modules in the cis-processing workflow."""
import os
import os.path as op
import subprocess

import pandas as pd


def run(command, env=None):
    """Run a given command with certain environment variables set."""
    merged_env = os.environ
    if env:
        merged_env.update(env)
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, shell=True,
                               env=merged_env)
    while True:
        line = process.stdout.readline()
        line = str(line, 'utf-8')[:-1]
        print(line)
        if line == '' and process.poll() is not None:
            break

    if process.returncode != 0:
        raise Exception("Non zero return code: {0}\n"
                        "{1}\n\n{2}".format(process.returncode, command,
                                            process.stdout.read()))


def clean_csv(in_file):
    """Convert NaNs to zeroes.

    Convert NaNs in a csv to zeros. Necessary for using the MRIQC
    classifier on datasets which may have skull-stripped data.
    This problem is discussed here (with no solution):
    https://github.com/poldracklab/mriqc/issues/546

    Writes out a file with the same name as the input file, but with the suffix
    "_cleaned".
    """
    fname = op.basename(in_file)
    d = op.dirname(in_file)
    fname, ext = op.splitext(fname)
    out_fname = fname + '_cleaned'
    out_file = op.join(d, out_fname + ext)

    df = pd.read_csv(in_file)
    df = df.fillna(0)
    df.to_csv(out_file, line_terminator='\n', index=False)
