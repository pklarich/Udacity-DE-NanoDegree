import psycopg2
import configparser
from sql_queries import data_check_1,

def data_check_1(cur, conn):
    """
    Checks each table to make sure they contain records
    """
    for query in data_check_1:
        cur.execute(query)
        results = cur.fetchone()
        print(results)

def data_check_2(cur,conn):
    """
    Previews data entry into each table to make sure it was loaded correctly
    """
    for query in data_check_2:
        cur.execute(query)
        results = cur.fetchone()
        print(results)

def main():
    """
    Performs data quality checks to ensure data is being processed properly
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
   
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    data_check_1(cur,conn)
    data_check_2(cur,conn)

    conn.close()

if __name__ == "__main__":
    main()
    
