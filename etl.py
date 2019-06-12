import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """
    Load Staging Tables function
    The function loads log and song data json file by importing copy_table_queries  
    from sql_queries.py.  The copy_table_queries uses the "copy" function to load the 
    data into the Redshift Cluster
    """    
    for query in copy_table_queries:
        loadTimes = []
        
        print('======= LOAD staging tables =======')
        print(query)         
        t0 = time()          
        cur.execute(query)              
        conn.commit()
        loadTime = time()-t0          
        loadTimes.append(loadTime)
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def insert_tables(cur, conn):
    """
    Load Insert Tables function
    The function insert data into the fact and dimension tables by importing insert_table_queries  
    from sql_queries.py. The fact and dimension table loads are based on the data that was loaded into 
    the staging tables.  This mean function: load_staging_tables must run first before insert_tables.
    """      
    for query in insert_table_queries:
        loadTimes = []

        print('======= LOAD Dimension and Fact tables =======')
        print(query)  
        t0 = time()          
        cur.execute(query)         
        conn.commit()
        loadTime = time()-t0          
        loadTimes.append(loadTime)
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()   

    load_staging_tables(cur, conn) 
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()