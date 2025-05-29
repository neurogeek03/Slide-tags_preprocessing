"""
Title:        CurioTrekker Automation
Description:  Running CurioTrekker for 1 or multiple samples.
Author:       Hugo Hudson & Maria Eleni Fafouti
Date:         15-04-2025
"""

# ========== IMPORTS ==========
import os
import subprocess
from datetime import datetime
import pandas as pd
import csv
import numpy as np 

# EDIT HERE: ========== INPUT YOUR PATHS TO EACH ==========
email = "mariaeleni.fafouti@mail.utoronto.ca"
cb_path = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CB_out"
cr_path = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CR_out"
fastq_path = "/home/mfafouti/nearline/rrg-shreejoy/SlideTagData/GOY29355.20241128" # note: container might require files to be in local folder
tile_folder = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/tiles"
sample_output = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CT_out"

# EDIT HERE: ========== TYPE THE LIST OF YOUR SAMPLES ==========
# It can be 1 or multiple samples. However, each sample usually has specific resource demands which can differ from others. 
# Hence some samples might need to be ran individually.
sample_list = ["BC13"]
tile_list = ["U0010_008_BeadBarcodes.txt"]

for i, sample in enumerate(sample_list):
    # Defining the directory where we saved the 10x stype sc output
    sc_outdir = os.path.join(cb_path, sample, 'sc_out') # this is created with a different script, after CellBender

    # Locate the raw feature matrix file dynamically
    os.makedirs(os.path.join(cb_path, sample), exist_ok=True)

    # Define parameters
    CURRENT_DATE = datetime.now().strftime('%Y%m%d_%H%M%S')
    TILE_ID = os.path.join(tile_folder, tile_list[i])
    fastq_sample = os.path.join(fastq_path, sample, "SP")
    dir_list = sorted(os.listdir(fastq_sample))
    FASTQ_1 = os.path.join(fastq_path, 
        sample, 
        "SP", 
        next(file for file in os.listdir(os.path.join(fastq_path, sample, "SP")) 
            if file.endswith("R1_001.fastq.gz"))) # fastq files should be merged, this selects the merged R1 file
    FASTQ_2 = os.path.join(fastq_path, 
        sample, 
        "SP",
        next(file for file in os.listdir(os.path.join(fastq_path, sample, "SP")) 
            if file.endswith("R2_001.fastq.gz"))) # fastq files should be merged, this selects the merged R2 file
    
    print(fastq_sample)
    print(f"dir_list: {dir_list}")
    
    # Locating 
    CELLRANGER_OUTPUT_PREFIX = os.path.join(cr_path, sample)
    middle_folder = os.listdir(CELLRANGER_OUTPUT_PREFIX)[0]  # Assuming only one folder
    CELLRANGER_OUTPUT = os.path.join(CELLRANGER_OUTPUT_PREFIX, middle_folder, "outs", "filtered_feature_bc_matrix")
    SC_OUT = sc_outdir

    # Sampleshheet header
    table_header = ["sample", "sample_sc", "experiment_date", "barcode_file", "fastq_1", "fastq_2", "sc_outdir", "sc_platform", "profile", "subsample", "cores"]
    table_content = [f"{sample}",f"{sample}_sc",f"{CURRENT_DATE}",f"{TILE_ID}",f"{FASTQ_1}",f"{FASTQ_2}",f"{SC_OUT}","TrekkerU_C","singularity","no","16"]
    pd.DataFrame([table_header, table_content]).to_csv(
        f"{os.path.join(sample_output, 'withBenderSamplesheet')}_{sample}_samplesheet_trekker.csv", 
        header=False, 
        index=False)

    # Grab samplesheet path
    samplesheet_name = f"withBenderSamplesheet_{sample}_samplesheet_trekker.csv"
    samplesheet_path = os.path.join(sample_output, samplesheet_name)

    # ========== CREATING SBATCH SCRIPT ========== 
    # SLURM job script for sbatch submission with email notifications
    sbatch_script = f"""#!/bin/bash
        #SBATCH --job-name=ct_{sample}   # Name of your job
        #SBATCH --account=def-shreejoy                # Compute Canada account
        #SBATCH --time=15:30:00                        # Job time (HH:MM:SS)
        #SBATCH --nodes=1                              # Number of nodes
        #SBATCH --ntasks=1                             # Number of tasks
        #SBATCH --cpus-per-task=16                      # Request 4 CPU cores per task
        #SBATCH --mem=150G                              # Request 64 GB of memory
        #SBATCH --output={os.path.join(sample_output, f'ct_{sample}%j.log')}
        #SBATCH --output={os.path.join(sample_output, f'ct_{sample}%x_%j.out ')}
        #SBATCH --error={os.path.join(sample_output, f'ct_{sample}%x_%j.err ')}
        #SBATCH --mail-user={email}
        #SBATCH --mail-type=BEGIN,END,FAIL

        module load StdEnv/2023                     # Load default environment
        module load apptainer/1.3.4   

        cd /home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/CurioTrekker/curiotrekker-v1.1.0
        bash /home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/CurioTrekker/curiotrekker-v1.1.0/nuclei_locater_toplevel.sh {samplesheet_path} 
        """

    # Write the SLURM script to a file
    sbatch_script_path = os.path.join(sample_output, f"withBendersbatch_curiotrekker_{sample}.slurm")
    with open(sbatch_script_path, "w") as f:
        f.write(sbatch_script)

    # ========== SUBMITTING AS A JOB ========== 
    try:
        subprocess.run(["sbatch", sbatch_script_path], check=True)
        print(f"Submitted Curio Trekker job for {sample}. SLURM script: {sbatch_script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit Curio Trekker job for {sample} with CellBender. Error: {e}")
        with open(log_file, "a") as log:
            log.write(f"Failed to submit Curio Trekker job. Error: {e}\n")
