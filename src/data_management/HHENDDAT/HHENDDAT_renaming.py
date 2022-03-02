# Aslında bu file formatında yazmaya çalıştım ama nedense run etmiyor. Sende ediyor mu??
# Gerçi bunu projeye koymasak da olur --ays
"""
This file contains code to create list of
variables in PPENDDAT dataset.
"""
import numpy as np
import pandas as pd

penddat = pd.read_stata("../../original_data/PENDDAT_cf_W11.dta")
np.savetxt("penddat_renaming.csv", list(penddat), delimiter=" ;", fmt="% s")
