from canary import Canary


# main.py
import os
from pathlib import Path

from canary import Canary

if __name__ == "__main__":
    canary = Canary()
    canary.run()
