# dnp-create-paneldata
Python tool to organize panel data for DNP (Deutsches Normungspanel)

## Overview
This is a python script that automates the panel data preparation step for survey data collected in several waves (with potentially changing variable names, question scales, participants & participant-affiliations, etc.):

1. Unify variable names
2. Map question scales
3. Concatenate yearly data into long format + variable "year"
4. Identify participants in project database, find yearly affiliations
5. Select ideal representative for companies (avoid duplicate entries)
6. Create data structure (info sheet, data dictionary, yearly data, panel data)
7. Output as Excel-file

The inputs for this script are:

- **`settings/settings.json`**: Main settings for running the script, e.g., paths to other input files, which years to include in output, rules for missing data, etc.
- **panel** (default: `settings/panel.xlsx`): Excel file that includes
  * data dictionary (variable names, variable aliases, variable properties, associated scale)
  * scales (data value aliases that are converted in script's step 2)
  * participant/company database (DB tables `persons`, `panel_entities`, `companies`, `groups`)
  * historic selections (which persons were in samples in the past, to reproduce results)
- **survey results** (default: Excel files ``survey-results/results_yyyy.xlsx`)

The output is an Excel file (default: `out/dnp_panel-data_<year_panel_starts>-<year_panel_ends>_<file_creation_date>.xlsx`) that includes several sheets:

- **info**: creation date, number of observations
- **variables**: variable overview (name, label, question, scale)
- **scales**: available data values per question scale
- **panel yyyy-yyyy**: panel data in long format with variable `year`
- **yyyy**: one sheet of data per year

## Run
Set up the configuration by editing `settings/settings.json` and then run using the run.cmd or `python create_dataset.py`.

## Configuration
See `settings/settings.json`.
