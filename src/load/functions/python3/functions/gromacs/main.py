import time
import wget
import os
import zipfile
import subprocess
import glob

# gromacs image CPU: docker.io/sakbhatt/gromacs-cpu-iluvatar-action:latest
# gromacs image GPU:
# example args: {"URL": "https://example.com/data.zip", "nsteps": 1000, "mode": "cpu"}
# mode can be skipped or kept as "cpu" or "gpu"
cold = True

def main(args):
    print(args)
    global cold
    was_cold = cold

    temp_dir = '/tmp'
    zip_filename = 'data.zip'
    outfile = os.path.join(temp_dir, zip_filename)
    

    try:
        downloaded_file = wget.download(args['URL'], out=outfile)
        print(downloaded_file, "was downloaded")
    except Exception as e:
        return {"error": f"Download failed: {e}"}
    try:
        with zipfile.ZipFile(outfile, 'r') as zip_ref:
            print('Extracting file to %s' % temp_dir)
            zip_ref.extractall(temp_dir)
    except Exception as e:
        return {"error": f"Extraction failed: {e}"}
    os.chdir(temp_dir)
    
    # Look for a .tpr file in the extracted files instead of using data.tpr
    tpr_files = glob.glob("*.tpr")
    if not tpr_files:
        return {"error": "No .tpr file found in the extracted files."}
    tpr_file = tpr_files[0]
    base_cmd = f"gmx mdrun -s {tpr_file} -nsteps {args['nsteps']}"
    
    # Determine extra flags based on the mode: CPU, GPU, or auto.

    # From documentation
    # -nb Used to set where to execute the short-range non-bonded interactions. 
    # Can be set to “auto,” “cpu” or “gpu.” Defaults to “auto,” which uses a 
    # compatible GPU if available. Setting “cpu” requires that no GPU is used. 
    # Setting “gpu” requires that a compatible GPU is available and will be used.

    # -pme Used to set where to execute the long-range non-bonded interactions. 
    # Can be set to “auto,” “cpu” or “gpu.” Defaults to “auto,” which uses a 
    # compatible GPU if available. Setting “gpu” requires that a compatible GPU 
    # is available. Multiple PME ranks are not supported with PME on GPU, so if 
    # a GPU is used for the PME calculation -npme must be set to 1.
    mode = args.get('mode', 'auto').lower()
    if mode == "cpu":
        extra_flags = " -nb cpu -pme cpu"
    elif mode == "gpu":
        extra_flags = " -nb gpu -pme gpu"
    else:
        extra_flags = ""
    
    cmd = base_cmd + extra_flags
    print("Executing command:", cmd)
    
    start_time = time.time()
    try:
        p = subprocess.run(cmd, shell=True, 
        check=True,
        capture_output=True,
        text=True
        )
    except subprocess.CalledProcessError as e:
        end_time = time.time()
        return {
            "error": f"Command failed with error {e}",
            "cold": was_cold,
            "start": start_time,
            "end": end_time,
            "base_cmd": base_cmd,
            "latency": end_time - start_time
        }
    end_time = time.time()
    full_output = (p.stdout or "") + (p.stderr or "")

    return {
        "cold": was_cold,
        "start": start_time,
        "end": end_time,
        "latency": end_time - start_time,
        "output": full_output
    }