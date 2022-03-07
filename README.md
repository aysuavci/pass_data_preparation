# PASS Campus File (PASS-CF) Data Preparation

by 
Aysu Avcı and Melih Damar

## PASS Data
- The Panel Study Labour Market and Social Security (PASS) is a data set established in 2007 by the Institute for Employment Research (IAB) among German households.
- The dataset contains information at the household and individual levels.
- Households are identified via ‘hnr’ and ‘wave’
- Individuals are identified via ‘pnr’ and ‘wave’
- Access to the PASS main dataset is only possible via an application to the Research Data Center (FDZ).

## PASS-CF Data
- PASS Campus file dataset is a simplified version of the main dataset that is suitable for academic teaching and obtaining various insights into the handling of PASS data.
- Compared to the main dataset, PASS-CF contains a reduced number of observations, range of variables and modified identification numbers as well as information; therefore it is not suitable for substantial scientific analysis.

## Motivation for the Project
- The purpose of this project is to create a PASS-CF data preparation repository that can be a template and a starting point for a similar repository for the main PASS data set.
- We also aim to familiarize ourselves with the effective use of programming in cleaning panel data sets and performing initial analysis.

## Setup
- The dataset PASS-CF is accessable after filling the form in the following link :[https://fdz.iab.de/en/campus-files/pass_cf/registrierungsformular-zum-download-des-campus-files-pass-0617-v1.aspx]
- The longitudinal PASS-CF datasets, 'HHENDDAT_cf_W11.dta' and 'PENDDAT_cf_W11.dta' are using in this project. Therefore, please add these data files into the folder `original-data/` in your local repository on your computer.
- Please make sure you have your conda environment up to date. The basic requirements can be found in the `environment.yml` file.

- run `conda develop .`
- run `pytask`

This resource can be helpful to get an understanding of pytask: [https://pytask-dev.readthedocs.io/en/latest/index.html](https://pytask-dev.readthedocs.io/en/latest/index.html)

## Structure

--- TO BE ADDED ---

The repository only contains scripts. The raw files need to be provided manually in the `src/original-data` folder and all output files need to be produced by running pytask and can then be found under `bld`.

See [https://econ-project-templates.readthedocs.io/en/stable/](https://econ-project-templates.readthedocs.io/en/stable/) for more information on the template that is used.

--- HOW TO GIVE CREDIT TO SOEP REPO? ---

[The LISS data management documentation] (https://liss-data-management-documentation.readthedocs.io/en/latest/) that was created with similar structure might be also helful.

## Implementation Details

The script performs the following steps for both household and individual level datasets.
1. Collect the respective .dta file.
2. Rename all variables according to the respective renaming csv file.
3. Perform basic data cleaning.
4. Reverse coding variables and aggregation.
5. Create dummies that might come in useful.
6. Save the final data sets as .pickle.
7. Perform some tests for used functions.
8. Report some summary statistics.

All the data cleaning steps-from step 1 to 6- are specified in `src/data_management/task_cleaning.py'.

## Renaming Files

- Can be found ...--TO BE ADDED--
- The renaming files are ";"-separated .csv files and specify the new name for each variable.
-- TO BE ADDED --
- Since the respective .csv files contains all the variables in that data set with the new variable names, it might be an useful documentation to view all the variables.
- The general information about the original naming of the datasets can be found in Table 21 of the PASS User Guide which can be dowloaded via the following link: [https://doku.iab.de/fdz/pass/FDZ-Datenreporte_PASS_EN.zip].

Some standardizations we use in renaming:
1. Use of English
2. A common naming for the variables in the same module (e.g. 'big_5').
3. All the negatively phrased variables ends with '_n'.

### Basic Data Cleaning

- As the basic step for cleaning we convert all the values coded as negative to NaN values (e.g. “I don’t know -> np.nan”).
- Then, we set indices for both data sets.

### Reverse coding and aggregation

- New variables are created according to the [PASS Scale and Instrument Manual]:(https://doku.iab.de/fdz/reporte/2020/MR_07-20_EN.pdf).
- Like deprivation module in the household level data, some variables are already aggreagated and can be found in the data.
- We extent this practice to the following modules in the individual level data:
1. Big Five
2. Effort-Reward Imbalance Scale (ERI Scale)
3. Gender Role Attitudes
- All the negatively phrased variables are inverted before the aggregation.
- All the newly created variables are named according to module name.

### Creating dummy variables

- All the variables we use to create dummies are specified in 'src/data_management/dummies/{data_name}_dummies.yaml'.
- The type of questions/variables that ...




