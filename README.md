# PASS Campus File (PASS-CF) Data Preparation

by 
Aysu Avcı and Melih Damar

## PASS Data
- The Panel Study Labour Market and Social Security (PASS) is a data set established in 2007 by the Institute for Employment Research (IAB) among German households.
- The dataset contains information at the household and individual levels.
- Households are identified via `hnr` and `wave`
- Individuals are identified via `pnr` and `wave`
- Access to the PASS main dataset is only possible via an application to the Research Data Center (FDZ).

## PASS-CF Data
- PASS Campus file dataset is a simplified version of the main dataset that is suitable for academic teaching and obtaining various insights into the handling of PASS data.
- Compared to the main dataset, PASS-CF contains a reduced number of observations, range of variables and modified identification numbers as well as information; therefore it is not suitable for substantial scientific analysis.

## Motivation for the Project
- The purpose of this project is to create a PASS-CF data preparation repository that can be a template and a starting point for a similar repository for the main PASS data set.
- We also aim to familiarize ourselves with the effective use of programming in cleaning panel data sets and performing initial analysis.

## Setup
- The dataset PASS-CF is accessable after filling the form in the following link :[https://fdz.iab.de/en/campus-files/pass_cf/registrierungsformular-zum-download-des-campus-files-pass-0617-v1.aspx]
- The longitudinal PASS-CF datasets, `HHENDDAT_cf_W11.dta`, `PENDDAT_cf_W11.dta`, `hweights_cf_W11.dta` and  `pweights_cf_W11.dta ` are using in this project. Therefore, please add these data files into the folder `src/original-data/` in your local repository on your computer.
- Please make sure you have your conda environment up to date. The basic requirements can be found in the `environment.yml` file.
- It is recommended also to activate project environment by running `conda activate pass_data_preparation`.

- run `conda develop .`
- run `pytask`

This resource can be helpful to get an understanding of pytask: [https://pytask-dev.readthedocs.io/en/latest/index.html](https://pytask-dev.readthedocs.io/en/latest/index.html)

## Structure
- `src/original_data/` should contain the four dataset that are added to the folder by the user. For each `data_set` there should be a `{data_set}_renaming.csv` in the `src/data_management/`.
- `src/data_management/` contains all the files related to cleaning process. The functions used for cleaning steps can be found in the file `cleaning_functions.py`. As mentioned above this file also contains the reanaming documents under each `data_set/` folder. The creation of dummy variables requires a list of variables that will be used in the `create_dummies()` function, therefore in `dummies/` each `data_set` that requires such operation should have a `{data_set}_dummies.yaml`. The tests written for cleaning functions are in the `test_cleaning.py` file. And finally, the cleaning task itself can be found in `task_cleaning.py` which creates the new datasets in three steps. 
- After running the pytask the final data sets `PENDDAT_aggregated.pickle` and `HHENDDAT_aggregated.pickle` are created under `bld/`, as well as a merged alternative of the datasets `merge_clean.pickle`. 
- `src/final/` contains `task_stat.py`, the task needed to form summary statistics.
- Other tasks include `task_documentation.py` and `task_paper.py` which forms the `research_project.pdf` based on `research_paper.tex` and `{data_set}_sum_stat.tex`.

The repository only contains scripts. The raw files need to be provided manually in the `src/original-data` folder and all output files need to be produced by running pytask and can then be found under `bld`.

See [https://econ-project-templates.readthedocs.io/en/stable/](https://econ-project-templates.readthedocs.io/en/stable/) for more information on the template that is used.

This repository is inspired by the SOEP data preparation repository of Institute of Labor Economics (IZA).

[The LISS data management documentation](https://liss-data-management-documentation.readthedocs.io/en/latest/) that was created with similar structure might be also helful.

## Implementation Details

The script performs the following steps for both household and individual level datasets.
1. Collect the respective .dta file.
2. Rename all variables according to the respective renaming .csv file.
3. Perform basic data cleaning.
4. Reverse coding variables and aggregation.
5. Create dummies that might come in useful.
6. Merge the datasets.
7. Save the final data sets as .pickle.
9. Report some summary statistics and create reseach paper in pdf format.

All the data cleaning steps-from step 1 to 7- are specified in `src/data_management/task_cleaning.py`. 
The detailed information about all of the steps can be found below.

## Renaming Files
- For each `data_set` there should be a `{data_set}_renaming.csv` in the `src/data_management/`.
- The renaming files are ";"-separated .csv files and specify the new name for each variable.
- Since the respective .csv files contains all the variables in that dataset with the new variable names, it might be an useful documentation to view all the variables.
- The general information about the original naming of the datasets can be found in Table 21 of the PASS User Guide which can be dowloaded via the following link: [https://doku.iab.de/fdz/pass/FDZ-Datenreporte_PASS_EN.zip].

Some standardizations we use in renaming:
1. Use of English
2. A common naming for the variables in the same module (e.g. `big_5`).
3. All the negatively phrased variables ends with `_n`.

### Basic Data Cleaning

- As the basic step for cleaning we convert all the values coded as negative to NaN values (e.g. “I don’t know -> np.nan”).
- Then, we set indices for both data sets.

### Reverse Coding and Aggregation

- New variables are created according to the [PASS Scale and Instrument Manual](https://doku.iab.de/fdz/reporte/2020/MR_07-20_EN.pdf).
- Like deprivation module in the household level data, some variables are already aggreagated and can be found in the data. We extent this practice to the following modules in the individual level data:

1. Big Five
2. Effort-Reward Imbalance Scale (ERI Scale)
3. Gender Role Attitudes

- All the negatively phrased variables are inverted before the aggregation.
- All the newly created variables are named according to module name.

### Creating Dummy Variables

- All the variables we use to create dummies are specified in `src/data_management/dummies/{data_name}_dummies.yaml`.
- Dummy variables are created without changing the original variables or values.
- For convenience, we create dummy variable in the following structure `{original_variable_name}_dummy`. 
- In PASS-CF dataset, the questions with two possible answers were not coded as dummy variables but variables consist of values 1 and 2 (e.g. Yes=1, No=2). Therefore, we create dummy variables for the following type of items:

1. Yes/No questions (e.g. social media usage in the last 4 weeks)
2. Categorical questions with two possible answers (e.g. gender). 
- On top of these variables we also created dummies for:
3. `PG0100`, a numeric variable that ranges between 0-99 and indicates the number of doctor visits in the last 3 months. 
4. Financial reason dummies for the Deprivation Module. In this module, individuals were asked about owning certain goods or engaging in certain activities. In case the household answers no to an item, the household is asked if it is due to financial or other reasons. Therefore, we create dummies where the value 1 corresponds to not owning goods or engaging in activities for financial reasons (e.g., no car for financial reasons).

### Task Cleaning and Merging Datasets
- The `task_cleaning.py` is divided into three steps and at the end of each step a file with processed datas is formed:
1. `task_basic_cleaning` performs renaming, basic cleaning and indexing for each `data_set`; and returns `{data_set}_clean.pickle` to `bld/cleaned_data/`.
2. `task_aggregation_and_dummy` performs reverse coding, creating aggregated variables and dummy variables for `PENDDAT` and `HHENDDAT` and returns `{data_set}_aggregated.pickle` to `bld/aggregated_data/`.
3. `task_merging` first merges the aggregated `PENDDAT` and `HHENDDAT` datasets with their cleaned datasets `hweights` and `pweights` and produces the two `{data_set}_weighted.pickle` to `bld/weighted_data/`. Secondly, it merges this two weighted datasets and created `merged_clean.pickle` under `bld/final_data`.

We did not delete any of the newly formed dataset files during the intermediate steps to allow researchers to use their preferred dataset.
