### Loading the libraries
import sys
import os

### Specify the path where function files are stored
wd = os.getcwd()
srcpath = os.path.join(wd,"scripts")

### Add the path to the system path
sys.path.append(srcpath)

### Loading the custom libraries
from functions_etl_pipeline import extract_data, transform_data, load_data

### Running the ETL pipeline.
print("⏳ Executing the ETL pipeline...")

print(" ⏳ Extract...")
df_trials, df_conditions, df_interventions, df_sponsors, df_locations, df_tokens = extract_data()
print(" ✅ Extract complete!")

print(" ⏳ Transform...")
df_sponsors_unique, df_trials, df_conditions_unique, df_interventions_unique, df_trials_conditions, df_trials_interventions, df_locations, df_tokens = transform_data(df_trials, df_conditions, df_interventions, df_sponsors, df_locations, df_tokens)
print(" ✅ Transform complete!")

print(" ⏳ Load...")
load_data(df_sponsors_unique, df_trials, df_conditions_unique, df_interventions_unique, df_trials_conditions, df_trials_interventions, df_locations, df_tokens)
print(" ✅ Load complete!")
