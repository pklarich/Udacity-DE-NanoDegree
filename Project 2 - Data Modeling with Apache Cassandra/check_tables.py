import cassandra
import pandas as pd
from sql_queries import check_queries

def check_data(session):
    """
    check_data runs the queries that are necessary for our datasets.
    It returns the row data from the query and prints it for viewing.
    
    INPUT
    *session the session instance variable
    
    """

    # 
    for i,query in enumerate(check_queries,1):
        rows = session.execute(check_queries[query])
        print('Query #{}:\n{}:\n'.format(i,query))
        df = pd.DataFrame(list(rows))
        print('\n{}\n'.format(df))
    print('\nAll queries have ran\n')

def main():
    """
    main is the first function that runs and connects
    to our keyspace and defines the cluster and session.
    It then runs the check_data function and shuts down 
    our connections once the program is completed.
    
    """
    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()

    # Set our keyspace to sparkify
    session.set_keyspace('sparkify')
    print('Connected to sparkify keyspace. \n')
    
    check_data(session)
    
    session.shutdown()
    print('session shutdown')
    cluster.shutdown()
    print('cluster shutdown \n')
    
    print('Program \'check_tables\' has ended.')
    

if __name__ == "__main__":
    main()
       