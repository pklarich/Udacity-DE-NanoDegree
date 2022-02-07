import psycopg2
import configparser
from sql_queries import insert_table_queries, staging_copy_queries

def copyto_staging(cur,conn):
    """
    Copies information from the s3 bucket into the staging tables.
    """
    for query in staging_copy_queries:
        cur.execute(query)
        conn.commit()
        
def insert_records(cur,conn):
    """
    Inserts data from the staging tables into the dimensional model.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    """
    Reads the configuration file, connects to the database as defined by the dwh.cfg file,
    runs the two functions included in the program, then closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    copyto_staging(cur, conn)
    insert_records(cur, conn)
    
    conn.close()
    
if __name__=="__main__":
    main()