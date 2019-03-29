# dbcourse-mutualfunds
Module for the Manipulation and Analysis of Mutual Fund data (assets, returns and holdings)

## Module Objectives
### Lecture 1:
1. Download data and understand data structure
2. Clean the main data file & sub-set it to identify balanced US active equity funds
3. Aggregate the information in the identified sub-set by share classes

### Homework 1:
1. Think through ID mapping to ensure greatest possible data coverage
    - crsp_fundno = sub-fund (share classes)
    - crsp_cl_grp = fund (unit of interest)
    - crsp_portno = portfolio identifier (CRSP)
    - wficn = portfolio identifier (Thomson Reuters)
2. Create and forward fill a map derived from the obtained aggregate file to be used in sub-setting monthly returns & holdings information

### Lecture 2:
Utilizing the map developed in the homework
1. Sub-set, clean and aggregate monthly returns
2. Clean, sub-set and complement holdings data to retrieve comprehensive holdings information

### Homework 2:
1. Generate holdings & returns based performance measures
2. Aggregate all information into a monthly panel dataset to be used for analysis
3. Pick the baseline regression of a known mutual fund paper and replicate it


## How to access the data?
- Data can be downloaded using the Python API following the instructions found in: Lectures/1_1_Intro_&_Data.ipynb
- Alternatively a zipped file containing all the data can be found in: 
https://www.dropbox.com/sh/7zq340neph1uvkr/AABBRbitbuX3YwVDblf7Mz66a?dl=0

## Project Structure
### Functions
- Utilis.py: contains useful functions used in all scripts

### Lectures
- Lecture 1:
    - 1_1_Intro_&_Data.ipynb: Describes the data and the project
    - 1_2_DatasetCreation.ipynb: Loads and subsets mutual fund summary files (all and active equity)
    - 1_3_DatasetAggregation.ipynb: Starting from the active equity subset, it aggregates funds information for different share classes
- Lecture 2: 
    - 2_2_Returns.ipynb: coming soon...
    - 2_3_Holdings.ipynb: coming soon...

### Homeworks
- Homework 1: coming soon...
- Homework 2: coming soon...