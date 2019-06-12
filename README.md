Redshift Sparkify DB Cluster readme.

The purpose of the Sparkify Database is to gather and maintain activity on the sparkify app from the active users and provide analytics to gauge the use of the app.

Tech Specs:
Redshift Cluster
    The Redshift Cluster created with 4 nodes
        Database "sparkify" as created under the sparkifydw" schema
            Within the sparkify database the following tables were created:
                Staging Tables
                    staging_events
                    staging_songs
                Fact Table(s)
                    songplays
                Dimension Tables    
                    users
                    artists
                    songs
                    time
Additional Notes:
    The all the dimension and staging_events tables were designed to be ALL distribution style.
        Although this would increase the storage space, this will copy all tables on each node
        but ensures that all rows are collocated for join ulitized by the tables.
    Table staging_songs was designed with an EVEN distribution style.  The table is currently not used in joins but may
    in the future.   
    The following tables has "sortkey" assgined to there respective primary key(s).  The reason for the sortkey on the primary key is due
    to the frequent joins that will occur.
        1.time - sortkey on start_time   
        2.artists - sortkey on artist_id
        3.songs - sortkey on song_id

                    
Rollout and Load 
To create the database structure and load the data from the S3 bucket do the following:
Open jupyter notebook
Select python version  
Select Rollout.ipynb
    Run each command individually
        1. Run command %load_ext sql (Set the envrionment to run commands and sql statements)
        2. Run command %run create_tables.py (This is create the schema, database and tables)
        3. Run command %etl.py (The process will extract and load data to the neccessary tables)
            NOTE:  When the etl.py script is executed, it prints out the the sql statment that is currently 
                   being executed along with the length of time it took to execute the sql statement(s)
        4. Run the follow cell "Create a connection to the sparkifydw" and then run the cell "Validation Check"  
            This is a sql statement that unions all the tables with the total counts after the ETL is complete.
            This is a simiple validation that the data has loaded into the tables.
            Note: If the connection is lost after the etl.py ran and the sql statement can not execute,
                    then re-run steps 1,2, and 3.  The connection string to the database is within the script.
                   
Sample DashBoards
    Sample Dashboards are part of the Rollout.ipynb
        1.  Run the cell #### Sample DashBoards Part 1 ####
        2.  Then run cell #### Sample DashBoards Part 2 ####
                This provide simple analytics for the following:
                    1. Most Song Played
                    2. Total Users by Gender
                    3. Total by levels
                    4. Total level by Gender 