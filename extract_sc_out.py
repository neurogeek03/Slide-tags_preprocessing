"""
Title:        Convert CellBender output into 10x format
Description:  Create a folder containing barcodes.tsv.gz, features.tsv.gz, matrix.mtx
Author:       Maria Eleni Fafouti
Date:         21-04-2025
"""

# ========== IMPORTS ==========
import h5py
import numpy as np
import gzip
import os
from scipy.sparse import csc_matrix
from scipy.io import mmwrite

h5_file = '/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CB_out/BC13/output_file_filtered.h5'
output_dir = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CB_out/BC13/sc_out"

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

with h5py.File(h5_file, 'r') as f:
    # Barcodes
    barcodes = [b.decode('utf-8') for b in f['matrix/barcodes'][:]]
    with gzip.open(os.path.join(output_dir, 'barcodes.tsv.gz'), 'wt') as out:
        out.write('\n'.join(barcodes) + '\n')
    print("✅ Saved barcodes.tsv.gz")

    # Features
    ids = [i.decode('utf-8') for i in f['matrix/features/id'][:]]
    names = [n.decode('utf-8') for n in f['matrix/features/name'][:]]
    types = [t.decode('utf-8') for t in f['matrix/features/feature_type'][:]]
    with gzip.open(os.path.join(output_dir, 'features.tsv.gz'), 'wt') as out:
        for i, n, t in zip(ids, names, types):
            out.write(f"{i}\t{n}\t{t}\n")
    print("✅ Saved features.tsv.gz")

    # Matrix (build sparse CSC matrix)
    data = f['matrix/data'][:]
    indices = f['matrix/indices'][:]
    indptr = f['matrix/indptr'][:]
    shape = f['matrix/shape'][:]
    mat = csc_matrix((data, indices, indptr), shape=shape)
    print("✅ Constructed sparse CSC matrix")

    # Save uncompressed .mtx (temporary)
    mtx_path = os.path.join(output_dir, 'matrix.mtx')
    mmwrite(mtx_path, mat, field='integer')
    print("✅ Saved matrix.mtx")

    # Compress to .gz
    with open(mtx_path, 'rb') as f_in:
        with gzip.open(mtx_path + '.gz', 'wb') as f_out:
            f_out.writelines(f_in)
    print("✅ Compressed matrix.mtx.gz")

    # Remove uncompressed .mtx
    os.remove(mtx_path)
    print("✅ Deleted uncompressed matrix.mtx")

print("🎉 All files successfully saved to:", output_dir)
