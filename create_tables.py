import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_query, drop_schema_query


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def drop_schema(cur, conn):
    for query in drop_schema_query:
        cur.execute(query)
        conn.commit()
        
def create_schema(cur, conn):
    for query in create_schema_query:
        cur.execute(query)
        conn.commit()
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_schema(cur, conn)    
    create_schema(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()