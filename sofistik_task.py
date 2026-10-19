import argparse
import os, sys, subprocess
import pathlib


parser = argparse.ArgumentParser(description="SOFiSTiK Job submitter.")
parser.add_argument("--id", type=str, required=True, help="Job ID")
# parameters for MCMC simulation - material properties, load cases, etc. can be added here as needed
...



def main(args):
    job_id = args.id
    # create a job directory
    os.makedirs(f"mcmc/{job_id}", exist_ok=True)
    
    # create an input file for the SOFiSTiK job
    filepath = pathlib.Path(f"mcmc/{job_id}/input.dat")
    filepath.write_text("...")  # Write the input file content
    ...
    ...
    ...
    
    # Construct the command to run the SOFiSTiK job
    command = f"sps.exe -b -run {filepath} -o mcmc/{job_id}/output.log 2>> mcmc/{job_id}/error.log"
    try:
        # Execute the command
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Job {job_id} submitted successfully.")
        print("Output:", result.stdout.decode())
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit job {job_id}.")
        print("Error:", e.stderr.decode())


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)