"""
Title:        Cell Bender Automation
Description:  Running Cell Bender for 1 or multiple sample: from reading fastq files, all the way into submitting the cellranger process as a job on a SLURM HPC cluster. 
Author:       Hugo Hudson
Date:         15-04-2025
"""

# ========== IMPORTS ==========
import os
import subprocess
from datetime import datetime

# EDIT HERE: ========== INPUT YOUR PATHS TO EACH ==========
output_base = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CB_out" 
email = "maria.elenifafouti@mail.utoronto.ca"
CONTAINER_PATH = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/CellBender/cellbender_latest.sif"
BATCH_PATH = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CR_out" #path to CellRanger output

# EDIT HERE: ========== TYPE THE LIST OF YOUR SAMPLES ==========
# It can be 1 or multiple samples. However, each sample usually has specific resource demands which can differ from others. Hence some samples might need to be ran individually.
sample_list = ["BC13"]

# ========== AUTOMATED PART ==========
for sample in sample_list:
    # Locate the raw feature matrix file dynamically: Searching within the CR output
    sample_fastq_entry = os.path.join(BATCH_PATH, sample)
    # middle_folder = os.listdir(sample_fastq_entry)[0]  # Assuming only one folder (eg. BC13_20250404_110829) and no other files. 
    middle_folders = [f for f in os.listdir(sample_fastq_entry) if os.path.isdir(os.path.join(sample_fastq_entry, f))]
    print(middle_folders)

    # Assuming only one folder — use the first
    middle_folder = middle_folders[0]

    sample_fastq = os.path.join(sample_fastq_entry, middle_folder, "outs/raw_feature_bc_matrix.h5")
    
    # Define output directory for the sample
    sample_output = os.path.join(output_base, sample)
    log_file = os.path.join(sample_output, f"cellbender_{sample}.log")

    # Create output directory
    os.makedirs(sample_output, exist_ok=True)

    # Define sample ID with timestamp
    sample_id = f"{sample}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # ========== CREATING SBATCH SCRIPT, CELL BENDER COMMAND IS INCLUDED HERE ========== 
    # SLURM job script for sbatch submission with email notifications
    sbatch_script = f"""#!/bin/bash
        #SBATCH --job-name=cellbender_gpu_{sample}   # Name of your job
        #SBATCH --account=def-shreejoy                # Compute Canada account
        #SBATCH --time=03:00:00                        # Job time (HH:MM:SS)
        #SBATCH --nodes=1                              # Number of nodes
        #SBATCH --ntasks=1                             # Number of tasks
        #SBATCH --gres=gpu:1                           # Request 1 GPU
        #SBATCH --cpus-per-task=4                      # Request 4 CPU cores per task
        #SBATCH --mem=128G                              # Request 64 GB of memory
        #SBATCH --output={os.path.join(sample_output, 'cellbender_%j.log')}
        #SBATCH --mail-user={email}                    
        #SBATCH --mail-type=BEGIN,END,FAIL             
        
        # Load the Apptainer module
        module load StdEnv/2023                     # Load default environment
        module load apptainer/1.3.4                    # Adjust version if needed
        
        # Run Cell Bender
        apptainer exec --nv {CONTAINER_PATH} cellbender remove-background \\
            --input {sample_fastq} \\
            --output {os.path.join(sample_output, 'output_file.h5')} \\
            --cuda \\
            --epochs 150
        """

    # Write the SLURM script to a file
    sbatch_script_path = os.path.join(sample_output, f"sbatch_cellbender_{sample}.slurm")
    with open(sbatch_script_path, "w") as f:
        f.write(sbatch_script)

    # ========== SUBMITTING AS A JOB ========== 
    try:
        subprocess.run(["sbatch", sbatch_script_path], check=True)
        print(f"Submitted Cell Bender job for {sample}. SLURM script: {sbatch_script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit Cell Bender job for {sample}. Error: {e}")
        with open(log_file, "a") as log:
            log.write(f"Failed to submit Cell Bender job. Error: {e}\n")
