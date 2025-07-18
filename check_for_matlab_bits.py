import os
from shutil import which

if os.getenv("MLM_LICENSE_FILE"):
    print("MAtlab license file available")
else:
    print("Matlab license file not available")

if os.getenv("MLM_LICENSE_TOKEN"):
    print("MAtlab license token available")
else:
    print("Matlab license token not available")

if which("matlab") is None:
    print("Could not find matlab")
else:
    print("Found matlab")
