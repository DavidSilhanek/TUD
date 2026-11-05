import shutil
import argparse
import os
import sys
import subprocess
import glob
import pathlib

# import logging
# logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description="Run QMC calculations")
parser.add_argument("--path", type=str, required=True, help="Path to the QMC calculation directory")


def main(args):

    path = args.path
    if not os.path.exists(path):
        print(f"Error: The specified path '{path}' does not exist.")
        sys.exit(1)

    # Change to the specified directory
    os.chdir(path)

    # Create a clean logfile
    LOGFILE = os.path.join(path, "log.txt")
    if os.path.exists(LOGFILE): os.remove(LOGFILE)

    # Run the QMC calculation within SOFiSTiK
    path_to_sps = pathlib.Path("C:/Program Files/SOFiSTiK/2027/SOFiSTiK 2027/sps.exe")
    result = subprocess.run(
        f'"{path_to_sps}" -b -run openLAB_FE-Model.dat >> "{LOGFILE}" 2>&1',
        shell=True,
        check=True
    )
    
    #import time
    #time.sleep(5)
    
    # cleanup: Remove the temporary files and garbage generated during the calculation
    regexps = ["*_csm*.dat", "*.lst", "*.erg","*.prt", "*.plb", "*.sqlite", "*.cdb"]
    files = []
    for pattern in regexps:
        files.extend(glob.glob(pattern))
    for f in files:
        if os.path.exists(f):
            os.remove(f)

    # check the returncode
    if result.returncode != 0:
        print("Error: QMC calculation failed.")
        sys.exit(1)
    else:
        # check the output file for errors which are not reflected by sps.exe...
        with open(LOGFILE, "r") as f:
            for line in f:
                if "errors" in line.lower():
                    n_errs = int(line.split(" errors and")[0][-1])
                    if n_errs > 0:
                        print(n_errs)
                        print(f"Error: QMC calculation failed with {n_errs} errors.")
                        sys.exit(1)
                    
        # if no errors were found, print a success message and exit
        print("QMC calculation completed successfully.")
        sys.exit(0)

if __name__ == "__main__":
    args = parser.parse_args()
    path = args.path
    main(args)
