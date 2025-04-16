"""
Title:        Cell Ranger Automation
Description:  Running CellRanger for 1 or multiple sample: from reading fastq files, all the way into submitting the cellranger process as a job on a SLURM HPC cluster. 
Author:       Hugo Hudson
Date:         15-04-2025
"""

# ========== IMPORTS ==========
import os
import subprocess
from datetime import datetime

# EDIT HERE: ========== INPUT YOUR PATHS TO EACH ==========
cellranger_bin = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/CellRanger/cellranger-9.0.1/bin/cellranger"
fastq_base = "/home/mfafouti/nearline/rrg-shreejoy/SlideTagData/GOY29355.20241128"
ref_path = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/References/rat/refdata-gex-mRatBN7-2-2024-A"
output_base = "/home/mfafouti/scratch/Mommybrain_marlen/Slide_tags/SlideTagDataPipeline/Manual_processing/CR_out"
email = "mariaeleni.fafouti@mail.utoronto.ca" # used to receive email notifications 

# EDIT HERE: ========== TYPE THE LIST OF YOUR SAMPLES ==========
# It can be 1 or multiple samples. However, each sample usually has specific resource demands which can differ from others. Hence some samples might need to be ran individually.
sample_list = ["BC13"]

# ========== AUTOMATED PART ==========
for sample in sample_list:
    # Specify directory for the FASTQ files
    sample_fastq = os.path.join(fastq_base, sample, "cDNA")  # The FASTQ path still remains the same.
    sample_output = os.path.join(output_base, sample)
    log_file = os.path.join(sample_output, f"cellranger_{sample}.log")
    
    # Check if FASTQ directory exists
    if not os.path.isdir(sample_fastq):
        print(f"Error: FASTQ directory {sample_fastq} does not exist for sample {sample}. Skipping...")
        with open(log_file, "a") as log:
            log.write(f"Error: FASTQ directory {sample_fastq} does not exist. Skipping...\n")
        continue

    # Create output directory on scratch storage
    os.makedirs(sample_output, exist_ok=True)

    # Define sample ID with timestamp
    sample_id = f"{sample}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # ========== CELLRANGER COMMAND ==========  
    cmd = [
        cellranger_bin, "count",
        f"--id={sample_id}",
        f"--fastqs={sample_fastq}",  # Still pointing to the FASTQ location in the home directory.
        f"--transcriptome={ref_path}",
        "--create-bam=false"
    ]

    # ========== CREATING SBATCH SCRIPT ========== 
    # SLURM job script for sbatch submission with email notifications
    sbatch_script = f"""#!/bin/bash
#SBATCH --job-name=cellranger_{sample}        # Job name
#SBATCH --output={sample_output}/cellranger_{sample}_%j.log  # Output file name, %j is replaced by Job ID
#SBATCH --error={sample_output}/cellranger_{sample}_%j.err   # Error file name
#SBATCH --time=36:00:00                        # Set the time limit for the job (72 hours, adjust as needed)
#SBATCH --cpus-per-task=16                      # Number of CPUs per task (adjust as needed)
#SBATCH --mem=64G                              # Memory allocation (adjust as needed)
#SBATCH --mail-user={email}                    # Your email address
#SBATCH --mail-type=BEGIN,END,FAIL             # Email notifications on job start, end, or failure
#SBATCH --chdir={sample_output}              # Set the working directory to the sample output directory


# Run Cell Ranger
{ ' '.join(cmd) }
"""

    # Write the SLURM script to a file with .slurm extension
    sbatch_script_path = os.path.join(sample_output, f"sbatch_cellranger_{sample}.slurm")
    with open(sbatch_script_path, 'w') as f:
        f.write(sbatch_script)

    # ========== SUBMITTING AS A JOB ========== 
    try:
        subprocess.run(['sbatch', sbatch_script_path], check=True)
        print(f"Submitted Cell Ranger job for {sample}. SLURM script: {sbatch_script_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit Cell Ranger job for {sample}. Error: {e}")
        with open(log_file, "a") as log:
            log.write(f"Failed to submit Cell Ranger job. Error: {e}\n")
