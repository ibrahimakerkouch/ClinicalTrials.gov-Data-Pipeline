### Loading the libraries
import pandas as pd
import requests as res
import re
import psycopg2
from psycopg2 import sql
from progress.bar import Bar

def extract_data():
    """Extract data from the Rest API"""
    ## Defining the standard url string
    str_url_page = "https://clinicaltrials.gov/api/v2/studies?query.titles=cancer&pageSize=1000"

    ## Defining the connection string
    host = "localhost"        # or the server IP/address
    database = "CancerClinicalTrials"
    user = "postgres"
    password = "Admin147@_"
    port = "5432"             # Default PostgreSQL port

    ## Creating the engine using the connection string
    conn = psycopg2.connect(
        host=host, 
        database=database, 
        user=user, 
        password=password, 
        port=port)

    ## Opening the connection
    connection = conn.cursor()

    ## Retrieving the last token
    connection.execute("select nextPageToken from log_pages order by ctid desc limit 1;")
    result_pages = connection.fetchone()

    ## Verifying if the table is full 
    if result_pages is not None:
        str_url_page = str_url_page + "&pageToken=" + result_pages[0]

    ## Closing the connection
    connection.close()
        
    ## Retrieving data from the Rest API
    results = res.get(str_url_page)
    results_json = results.json()
    studies = results_json.get('studies', [])

    ## Initializing the progress bar
    bar = Bar('Processing', max=len(studies), suffix='%(percent)d%%')

    ## Defining the dataframes relevant to the database's tables
    df_trials = pd.DataFrame()
    df_conditions = pd.DataFrame()
    df_interventions = pd.DataFrame()
    df_sponsors = pd.DataFrame()
    df_locations = pd.DataFrame()

    ## Fetching the records
    for index, study in enumerate(studies):
        ### Trials
        nct_id = study['protocolSection']['identificationModule'].get('nctId', '')
        title = study['protocolSection']['identificationModule'].get('briefTitle', '')
        title = title[:1] + title[1:].lower()
        status = study['protocolSection']['statusModule'].get('overallStatus', '')
        status = status[:1] + status[1:].lower()
        phase = study['protocolSection']['designModule'].get('phases', [])
        phase = ','.join(phase)
        sponsor = study['protocolSection']['sponsorCollaboratorsModule']['leadSponsor'].get("name", '')
        df_trials_stagia = pd.DataFrame({
            "nct_id" : [nct_id.strip()],
            "title" : [title.strip()],
            "status" : [status.strip()],
            "phase" : [phase.strip()],
            "sponsor" : [sponsor.strip()],
            "exists" : [False]
        })
        df_trials = pd.concat([df_trials, df_trials_stagia], axis=0, ignore_index=True)

        ### Sponsors
        if (len(sponsor) > 0):
            df_sponsors_stagia = pd.DataFrame({
                "nct_id" : [nct_id],
                "name" : [sponsor.strip()]
            })
            df_sponsors = pd.concat([df_sponsors, df_sponsors_stagia], axis=0, ignore_index=True)

        ### Conditions
        conditions = study['protocolSection'].get('conditionsModule', [])
        
        if(len(conditions) > 0):
            conditions = conditions.get("conditions", [])
            df_conditions_stagia = pd.DataFrame({
                "nct_id" : [nct_id] * len(conditions),
                "name" : [x.strip() for x in conditions]
            })

            df_conditions = pd.concat([df_conditions, df_conditions_stagia], axis=0, ignore_index=True)
        
        ### Interventions
        if "armsInterventionsModule" in study['protocolSection']:
            interventions = study['protocolSection']['armsInterventionsModule'].get("interventions",[])
            if(len(interventions) > 0):
                for intervention in interventions:
                    intervention = intervention.get("name", "Unknown")
                    df_interventions_stagia = pd.DataFrame({
                        "nct_id" : [nct_id],
                        "name" : [intervention.strip()]
                    })
            
                    df_interventions = pd.concat([df_interventions, df_interventions_stagia], axis=0, ignore_index=True)

        ### Locations     
        if "contactsLocationsModule" in study["protocolSection"]:
            if "locations" in study["protocolSection"]["contactsLocationsModule"]:
                locations = study["protocolSection"]["contactsLocationsModule"]["locations"]
                if len(locations) > 0:
                    for location in locations:
                        
                        name = ""
                        address = ""
                        
                        if "facility" in location:
                            name = location["facility"].strip()
                        
                        if "zip" in location:
                            if "state" not in location:
                                address = location["city"]+", "+location["zip"]+" "+location["country"]
                            else:
                                address = location["city"]+", "+location["state"]+" "+location["zip"]+" "+location["country"]
                        else:
                            if "city" in location:
                                address = location["city"]+", "+location["country"]
                        
                        ### Trimming the address
                        address = re.sub("\s+", " ", address)
                        address = address.strip()
                        
                        df_locations_stagia = pd.DataFrame({
                            "nct_id" : [nct_id],
                            "name":[name.strip()],
                            "address":[address]
                        })
                        df_locations = pd.concat([df_locations, df_locations_stagia], axis = 0, ignore_index=True)
                        
        ### Updating the progress bar   
        bar.next()

    ## Finish the progress bar
    bar.finish()
        
    ## Fetching the records for tokens  
    token = results_json.get("nextPageToken", "")
    df_tokens = pd.DataFrame({"nextPageToken" : [token]})

    return(df_trials, df_conditions, df_interventions, df_sponsors, df_locations, df_tokens)
    
def transform_data(df_trials, df_conditions, df_interventions, df_sponsors, df_locations, df_tokens):
    """Transforming Data as needed"""
    ## Defining the connection string
    host = "localhost"        # or the server IP/address
    database = "CancerClinicalTrials"
    user = "postgres"
    password = "Admin147@_"
    port = "5432"             # Default PostgreSQL port

    ## Creating the engine using the connection string
    conn = psycopg2.connect(
        host=host, 
        database=database, 
        user=user, 
        password=password, 
        port=port)

    ## Opening the connection
    connection = conn.cursor()

    ## Initializing the progress bar
    bar = Bar('Processing', max=11, suffix='%(percent)d%%')

    ## Verifying if the dataframe is full
    if len(df_trials) > 0:
        ### Retrieving the most recent trial_id from the 'trials' table
        connection.execute("select * from trials order by trial_id desc limit 1;")
        result_trials = connection.fetchone()  # Fetches the first row
        
        ### Verifying if the table is full
        trial_id = 1
        if result_trials is not None: 
            trial_id = result_trials[0] + 1

        ### Starting the increment from the most recent trial_id
        if trial_id == 1:
            df_trials["trial_id"] = list(range(trial_id, len(df_trials) + 1))
        else:
            df_trials["trial_id"] = list(range(trial_id, trial_id + len(df_trials)))

        ### Verifying if the records are existing
        for i in range(len(df_trials)):
            query = sql.SQL("select trial_id from trials where nct_id = %s").format(
                column=sql.Identifier('nct_id')
            )
            connection.execute(query, (df_trials.loc[i,"nct_id"],))
            rs_trial = connection.fetchone()
            if rs_trial is not None:
                df_trials.loc[i,"trial_id"] = rs_trial[0]
                df_trials.loc[i, "exists"] = True
        
    ### Updating the progress bar
    bar.next()
    
    ## Verifying if the dataframe is full
    if len(df_conditions) > 0:
        ### Retrieving unique values from the "name" column
        df_conditions_unique = pd.DataFrame({"name":df_conditions["name"].drop_duplicates()})
        df_conditions_unique = df_conditions_unique.reset_index(drop=True)
        df_conditions_unique["exists"] = False
        
        ### Retrieving the most recent condition_id from the 'conditions' table
        connection.execute("select * from conditions order by condition_id desc limit 1;")
        result_conditions = connection.fetchone()  # Fetches the first row

        ### Verifying if the table is full  
        condition_id = 1
        if result_conditions is not None: 
            condition_id = result_conditions[0] + 1

        ## Starting the increment from the most recent condition_id
        if condition_id == 1:
            df_conditions_unique["condition_id"] = list(range(condition_id, len(df_conditions_unique) + 1))
        else:
            df_conditions_unique["condition_id"] = list(range(condition_id, condition_id + len(df_conditions_unique)))
            
        ## Verifying if the records are existing
        for i in range(len(df_conditions_unique)):
            query = sql.SQL("select condition_id from conditions where name = %s").format(
                column=sql.Identifier('name')
            )
            connection.execute(query, (df_conditions_unique.loc[i,"name"],))
            
            rs_condition = connection.fetchone()
            if rs_condition is not None:
                df_conditions_unique.loc[i,"condition_id"] = rs_condition[0]
                df_conditions_unique.loc[i,"exists"] = True
    else:
        df_conditions_unique = pd.DataFrame()
    
    ### Updating the progress bar
    bar.next()
    
    ## Verifying if the dataframe is full
    if len(df_interventions) > 0:
        ### Retrieving unique values from the "name" column
        df_interventions_unique = pd.DataFrame({"name":df_interventions["name"].drop_duplicates()})
        df_interventions_unique = df_interventions_unique.reset_index(drop = True)
        df_interventions_unique["exists"] = False
        
        ### Retrieving the most recent intervention_id from the 'interventions' table
        connection.execute("select * from interventions order by intervention_id desc limit 1;")
        result_interventions = connection.fetchone()  # Fetches the first row
        
        ### Verifying if the table is full 
        intervention_id = 1
        if result_interventions is not None: 
            intervention_id = result_interventions[0] + 1

        ## Starting the increment from the most recent intervention_id
        if intervention_id == 1:
            df_interventions_unique["intervention_id"] = list(range(intervention_id, len(df_interventions_unique) + 1))
        else:
            df_interventions_unique["intervention_id"] = list(range(intervention_id, intervention_id + len(df_interventions_unique)))
            
        ## Verifying if the records are existing
        for i in range(len(df_interventions_unique)):
            query = sql.SQL("select intervention_id from interventions where name = %s").format(
                column=sql.Identifier('name')
            )
            connection.execute(query, (df_interventions_unique.loc[i,"name"],))
            
            rs_intervention = connection.fetchone()
            if rs_intervention is not None:
                df_interventions_unique.loc[i,"intervention_id"] = rs_intervention[0]
                df_interventions_unique.loc[i,"exists"] = True
    else:
        df_interventions_unique = pd.DataFrame()
    
    ### Updating the progress bar
    bar.next()
    
    ## Verifying if the dataframe is full
    if len(df_sponsors) > 0:
        ### Retrieving unique values from the "name" column
        df_sponsors_unique = pd.DataFrame({"name":df_sponsors["name"].drop_duplicates()})
        df_sponsors_unique = df_sponsors_unique.reset_index(drop = True)
        df_sponsors_unique["exists"] = False
        
        ### Retrieving the most recent location_id from the 'sponsors' table
        connection.execute("select * from sponsors order by sponsor_id desc limit 1;")
        result_sponsors = connection.fetchone()

        ### Verifying if the table is full 
        sponsor_id = 1
        if result_sponsors is not None:
            sponsor_id = result_sponsors[0] + 1

        ### Starting the increment from the most recent sponsor_id
        if sponsor_id == 1:
            df_sponsors_unique["sponsor_id"] = list(range(sponsor_id, len(df_sponsors_unique) + 1))
        else:
            df_sponsors_unique["sponsor_id"] = list(range(sponsor_id, sponsor_id + len(df_sponsors_unique)))
            
        ### Verifying if the records are existing
        for i in range(len(df_sponsors_unique)):
            query = sql.SQL("select sponsor_id from sponsors where name = %s").format(
                column=sql.Identifier('name')
            )
            connection.execute(query, (df_sponsors_unique.loc[i,"name"],))
            
            rs_sponsor = connection.fetchone()
            if rs_sponsor is not None:
                df_sponsors_unique.loc[i,"sponsor_id"] = rs_sponsor[0]
                df_sponsors_unique.loc[i, "exists"] = True
    else:
        df_sponsors_unique = pd.DataFrame()
        
    ### Updating the progress bar
    bar.next()
    
    ## Verifying if the dataframe is full
    if len(df_locations) > 0:
        ### Dropping the duplicates records
        df_locations = df_locations.drop_duplicates()
        
        ### Retrieving the most recent location_id from the 'locations' table
        connection.execute("select * from locations order by location_id desc limit 1;")
        result_locations = connection.fetchone()
        
        ### Verifying if the table is full 
        location_id = 1
        if result_locations is not None:
            location_id = result_locations[0] + 1

        ### Starting the increment from the most recent sponsor_id
        df_locations = df_locations.copy()
        if location_id == 1:
            df_locations["location_id"] = list(range(location_id, len(df_locations) + 1))
        else:
            df_locations["location_id"] = list(range(location_id, location_id + len(df_locations)))
    
    ### Updating the progress bar
    bar.next()
    
    ## Closing the connection
    connection.close()

    ## Merging dataframes on the primary key(s) to preserve unique identifiers
    ### Sponsors
    df_sponsors_unique = df_sponsors_unique[["sponsor_id", "name", "exists"]]
    df_sponsors = pd.merge(df_sponsors_unique, df_sponsors, on="name")
    
    ### Updating the progress bar
    bar.next()
    
    ### Trials and Sponsors
    df_trials = pd.merge(df_trials, df_sponsors, on="nct_id")
    df_trials = df_trials.drop(columns = ["name","exists_y"])
    
    ### Updating the progress bar
    bar.next()
    
    ### Trials and Conditions
    df_trials_conditions = pd.merge(df_trials, df_conditions, on="nct_id")
    df_trials_conditions = df_trials_conditions[["trial_id", "name"]]
    df_trials_conditions = pd.merge(df_trials_conditions, df_conditions_unique, on="name")
    df_trials_conditions = df_trials_conditions[["trial_id", "condition_id"]]
    df_trials_conditions = df_trials_conditions.drop_duplicates()
    
    ### Updating the progress bar
    bar.next()
    
    ### Trials and Interventions
    df_trials_interventions = pd.merge(df_trials, df_interventions, on="nct_id")
    df_trials_interventions = df_trials_interventions[["trial_id", "name"]]
    df_trials_interventions = pd.merge(df_trials_interventions, df_interventions_unique, on="name")
    df_trials_interventions = df_trials_interventions[["trial_id", "intervention_id"]]
    df_trials_interventions = df_trials_interventions.drop_duplicates()
    
    ### Updating the progress bar
    bar.next()
    
    ### Locations
    df_locations = pd.merge(df_trials, df_locations, on="nct_id")
    df_locations = df_locations[['trial_id', 'location_id', 'name', 'address']]
    
    ### Updating the progress bar
    bar.next()
    
    ## Resetting the indexes
    df_sponsors_unique = df_sponsors_unique.reset_index(drop=True)
    df_trials = df_trials.reset_index(drop=True)
    df_conditions_unique = df_conditions_unique.reset_index(drop=True)
    df_interventions_unique = df_interventions_unique.reset_index(drop=True)
    df_trials_conditions = df_trials_conditions.reset_index(drop=True)
    df_trials_interventions = df_trials_interventions.reset_index(drop=True)
    df_locations = df_locations.reset_index(drop=True)
    df_tokens = df_tokens.reset_index(drop=True)
    
    ### Updating the progress bar
    bar.next()
    
    ## Finish the progress bar
    bar.finish()
    
    return(df_sponsors_unique, df_trials, df_conditions_unique, df_interventions_unique, df_trials_conditions, df_trials_interventions, df_locations, df_tokens)

def load_data(df_sponsors_unique, df_trials, df_conditions_unique, df_interventions_unique, df_trials_conditions, df_trials_interventions, df_locations, df_tokens):
    """Storing the Data into the database"""
    ## Connecting to the database
    ### Defining the connection string
    host = "localhost"        # or the server IP/address
    database = "CancerClinicalTrials"
    user = "postgres"
    password = "Admin147@_"
    port = "5432"             # Default PostgreSQL port

    ### Creating the engine using the connection string
    conn = psycopg2.connect(
        host=host, 
        database=database, 
        user=user, 
        password=password, 
        port=port)

    ### Opening the connection
    connection = conn.cursor()
    
    ## Initializing the progress bar
    bar = Bar('Processing', max=8, suffix='%(percent)d%%')
    
    ## Sponsors
    ### Defining the personalized insert query
    insert_query_sponsors = """ INSERT INTO sponsors (sponsor_id, name) VALUES (%s, %s) """
    
    ### Inserting the records
    for i in range(len(df_sponsors_unique)):
        if df_sponsors_unique.loc[i,"exists"] == False:
            connection.execute(
                insert_query_sponsors,
                (int(df_sponsors_unique.loc[i,"sponsor_id"]), df_sponsors_unique.loc[i,"name"])
            )
            ### Saving the inserting      
            conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Trials
    ### Defining the personalized update query
    update_query_trials = sql.SQL("update trials set title=%s, status=%s, phase=%s, sponsor_id=%s where nct_id = %s").format(
        column=sql.Identifier('nct_id')
    )
    
    ### Defining the personalized insert query
    insert_query_trials = sql.SQL("INSERT INTO trials (trial_id, nct_id, title, status, phase, sponsor_id) VALUES (%s, %s, %s, %s, %s, %s)").format(
        column=sql.Identifier('nct_id')
    )
            
    ### Inserting and updating the records
    for i in range(len(df_trials)):
        if df_trials.loc[i,"exists_x"] == True: 
            connection.execute(update_query_trials,
                    (df_trials.loc[i,"title"],
                     df_trials.loc[i,"status"],
                     df_trials.loc[i,"phase"],
                     int(df_trials.loc[i,"sponsor_id"]),
                     df_trials.loc[i,"nct_id"])
                    )
            ### Saving the inserting and updating
            conn.commit()
        else:
            connection.execute(
                    insert_query_trials,
                    (int(df_trials.loc[i,"trial_id"]), 
                     df_trials.loc[i,"nct_id"],
                     df_trials.loc[i,"title"],
                     df_trials.loc[i,"status"],
                     df_trials.loc[i,"phase"],
                     int(df_trials.loc[i,"sponsor_id"]))
                     )
            ### Saving the inserting and updating
            conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Conditions
    ### Defining the personalized insert query
    insert_query_conditions = """ INSERT INTO conditions (condition_id, name) VALUES (%s, %s) """
    
    ### Inserting the records
    for i in range(len(df_conditions_unique)):
        if df_conditions_unique.loc[i,"exists"] == False:
            connection.execute(
                insert_query_conditions,
                (int(df_conditions_unique.loc[i,"condition_id"]), 
                df_conditions_unique.loc[i,"name"])
            )
            ### Saving the inserting   
            conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Interventions
    ### Defining the personalized insert query       
    insert_query_interventions = """ INSERT INTO interventions (intervention_id, name) VALUES (%s, %s) """

    ### Inserting the records       
    for i in range(len(df_interventions_unique)):
        if df_interventions_unique.loc[i,"exists"] == False:
            connection.execute(
                insert_query_interventions,
                (int(df_interventions_unique.loc[i,"intervention_id"]), 
                df_interventions_unique.loc[i,"name"])
            )
            ### Saving the inserting           
            conn.commit()
    
    ### Updating the progress bar
    bar.next()   
    
    ## Locations
    ### Defining the personalized insert query       
    insert_query_locations = """ INSERT INTO locations (location_id, trial_id, name, address) VALUES (%s, %s, %s, %s) """
    
    ### Inserting the records    
    for i in range(len(df_locations)):
        connection.execute(
            insert_query_locations,
            (int(df_locations.loc[i,"location_id"]), 
            int(df_locations.loc[i,"trial_id"]),
            df_locations.loc[i,"name"],
            df_locations.loc[i,"address"])
        )
        ### Saving the inserting    
        conn.commit()
    
    ### Updating the progress bar
    bar.next()  
    
    ## Trial_conditions
    ### Defining the personalized insert query
    insert_query_trials_conditions = """ INSERT INTO trials_conditions (trial_id, condition_id) VALUES (%s, %s) """

    ### Inserting the records 
    for i in range(len(df_trials_conditions)):
        connection.execute(
           insert_query_trials_conditions,
           (int(df_trials_conditions.loc[i,"trial_id"]), 
            int(df_trials_conditions.loc[i,"condition_id"]))
        )
        ### Saving the inserting    
        conn.commit()
    
    ### Defining the personalized delete query
    delete_duplicate_trials_conditions = """ WITH duplicates_con AS (
        SELECT 
            trial_id,
            condition_id,
            ROW_NUMBER() OVER (PARTITION BY trial_id, condition_id ORDER BY trial_id) AS row_num
        FROM 
            trials_conditions
    )
    DELETE FROM trials_conditions
    WHERE (trial_id, condition_id) IN (
        SELECT trial_id, condition_id FROM duplicates_con WHERE row_num > 1
    );"""

    ### Deleting the records
    connection.execute(delete_duplicate_trials_conditions)       
    
    ### Saving the inserting
    conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Trial_interventions
    ### Defining the personalized insert query    
    query_insert_trials_interventions = """ INSERT INTO trials_interventions (trial_id, intervention_id) VALUES (%s, %s) """

    ### Inserting the records    
    for i in range(len(df_trials_interventions)):
        connection.execute(
            query_insert_trials_interventions,
            (int(df_trials_interventions.loc[i,"trial_id"]), 
             int(df_trials_interventions.loc[i,"intervention_id"]))
        )
        ### Saving the inserting    
        conn.commit()
    
    ### Defining the personalized delete query    
    delete_duplicate_trials_interventions = """ WITH duplicates_intv AS (
        SELECT 
            trial_id,
            intervention_id,
            ROW_NUMBER() OVER (PARTITION BY trial_id, intervention_id ORDER BY trial_id) AS row_num
        FROM 
            trials_interventions
    )
    DELETE FROM trials_interventions
    WHERE (trial_id, intervention_id) IN (
        SELECT trial_id, intervention_id FROM duplicates_intv WHERE row_num > 1
    );"""
    
    ### Deleting the records
    connection.execute(delete_duplicate_trials_interventions)

    ### Saving the inserting    
    conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Log_pages  
    ### Defining the personalized insert query        
    insert_query_log_pages = """ INSERT INTO log_pages (nextPageToken) VALUES (%s) """
    
    ### Inserting the records  
    for i in range(len(df_tokens)):
        connection.execute(
            insert_query_log_pages,
            (df_tokens.loc[i,"nextPageToken"],)
        )        
        ### Saving the inserting    
        conn.commit()
    
    ### Updating the progress bar
    bar.next()
    
    ## Finish the progress bar
    bar.finish()
    
    ## Disconnecting from the database    
    ### Closing the connection
    connection.close()
