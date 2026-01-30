-- Create the sponsors table
CREATE TABLE sponsors (
    sponsor_id int PRIMARY KEY,  -- Unique ID for each sponsor (e.g., pharmaceutical company)
    name VARCHAR(255) NOT NULL UNIQUE  -- Name of the sponsor (e.g., Pfizer)
);

-- Create the trials table
CREATE TABLE trials (
    trial_id int PRIMARY KEY,  -- int automatically generates an integer for each trial
    nct_id VARCHAR(15) NOT NULL,
	title VARCHAR(400) NOT NULL,
    status VARCHAR(50),
    phase VARCHAR(50),
    sponsor_id INT,  -- Foreign key referencing sponsors
    FOREIGN KEY (sponsor_id) REFERENCES sponsors(sponsor_id) ON DELETE CASCADE -- Ensure sponsor_id references a valid sponsor
);

-- Create the conditions table
CREATE TABLE conditions (
    condition_id int PRIMARY KEY,  -- Unique ID for each condition (e.g., cancer types)
    name VARCHAR(255) NOT NULL UNIQUE  -- The name of the condition (e.g., Lung Cancer)
);

-- Create the interventions table
CREATE TABLE interventions (
    intervention_id int PRIMARY KEY,  -- Unique ID for each intervention (e.g., drug or therapy)
    name VARCHAR(255) NOT NULL UNIQUE  -- The name of the intervention (e.g., Chemotherapy)
);

-- Create the locations table
CREATE TABLE locations (
    location_id int PRIMARY KEY,  -- Unique ID for each trial location
    trial_id INT,  -- Foreign Key referencing trials
    name VARCHAR(255) NOT NULL,  -- The name (e.g., Hospital),
	address VARCHAR(255) NOT NULL,  -- The address (e.g., City, Hospital)
    FOREIGN KEY (trial_id) REFERENCES trials(trial_id) ON DELETE CASCADE -- Ensure trial_id references a valid trial
);

-- Create mapping table for trial_conditions (many-to-many relationship)
CREATE TABLE trials_conditions (
    trial_id INT,  -- Foreign Key referencing trials
    condition_id INT,  -- Foreign Key referencing conditions
    PRIMARY KEY (trial_id, condition_id),
    FOREIGN KEY (trial_id) REFERENCES trials(trial_id) ON DELETE CASCADE,  -- If a trial is deleted, remove the associated rows
    FOREIGN KEY (condition_id) REFERENCES conditions(condition_id) ON DELETE CASCADE  -- If a condition is deleted, remove associated rows
);

-- Create mapping table for trial_interventions (many-to-many relationship)
CREATE TABLE trials_interventions (
    trial_id INT,  -- Foreign Key referencing trials
    intervention_id INT,  -- Foreign Key referencing interventions
    PRIMARY KEY (trial_id, intervention_id),
    FOREIGN KEY (trial_id) REFERENCES trials(trial_id) ON DELETE CASCADE,  -- If a trial is deleted, remove the associated rows
    FOREIGN KEY (intervention_id) REFERENCES interventions(intervention_id) ON DELETE CASCADE  -- If an intervention is deleted, remove associated rows
);

-- Create the locations table
CREATE TABLE log_pages (
    nextPageToken VARCHAR(255) NOT NULL  -- The token of the next page (e.g., NF0g5JWFmvY)
);