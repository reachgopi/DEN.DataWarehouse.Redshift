# Summary

This project creates a sparkify data warehouse in Amazon Redshift by loading the data from Amazon s3 into redhift staging tables. 
From staging tables the data is cleaned up and loaded into the analytics tables to do the data analysis. Infrastructure required for this project is created using the boto3 client.

# Input Data Set
Following data is the input dataset available in S3 Song data (s3://udacity-dend/song_data) and Log data (s3://udacity-dend/log_data)

Song Dataset is a subset of real data from the Million Song Dataset. It's a JSON contains metadata about a song and artist of that song. 

Log Dataset consist of imaginary streaming app (Sparkify) log files in a JSON format generated by event simulator. Log files are partitioned by year and Month

# Datawarehouse Tables
    Log and song datasets are loaded into following amazon redshift staging tables using the COPY Command
    
                    1. log_events_staging
                    2.  songs_staging
    
    From the staging tables, data is cleaned up and loaded into the following analytics table for further analysis
                    1.  songplays
                    2.  users
                    3.  songs
                    4.  artists
                    5.  time

# Design Choice
    Based on the given input dataset following are considerably small 1. users 2. artists 3. songs so the data is loaded using distribution style all (diststyle all) so that the dataset available in all the clusters to perform join.

    Songplays is the facttable which is loaded with a distribution style key on starttime so that the data is partitioned in the cluster by the starttime

    Dimesion table time follows the same key distribution style on startime.

    Also sortkey is used on user id and startime to perform order by on user id and the starttime columns. 

# Running python scripts
        Following python scripts are available in the project and following scripts need to be invoked in the order
                1. python3 aws_iac.py
                2. python3 create_tables.py
                3. python3 etl.py
                4. python3 aws_iac.py "delete"

        1. aws_iac.py
            Accepts option parameter when provided with value "delete" performs delete operation when the option parameter is empty it will create the redshift role and cluster. Once cluster and role is created it also updates the dwh.cfg config file with the role and cluster information so no manual update is required.
                Usage : 
                    a. To Create Roles and clusters 
                            python3 aws_iac.py
                    b. To delete roles and clusters. 
                            python3 aws_iac.py "delete"
        2. create_tables.py
            Create Table first drops all the staging and analytical tables and then creates a new staging and analytic tables mentioned above.
                    python3 create_tables.py
        3. etl.py
            Etl script cleans up the data from staging tables and loads into analytic tables.

# ETL
    Most of the ETL is handled on the SQL level, here are the few important things performed on the ETL
    Empty User ID
            When the user id is empty then the ETL process loads the data with -1, we can also remove the data but for now this data is loaded into the analytics tables.
    Time Dimension table different fields
            to_char function is used to convert timestamp into the appropriate time formats on redshift Database.
    Loading Songplays data
            Playback is retrieved from Log json and also Left Outer join is performed with songstaging table to get the corresponding artist and song name from that table and inserted into songplays table.