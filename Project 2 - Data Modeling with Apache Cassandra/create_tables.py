import cassandra
from sql_queries import create_table_queries, drop_table_queries


def create_keyspace():
    """
    create_keyspace builds the sparkify keyspace in the cassandra 
    
    RETURNS:
    * cluster the cassandra instance
    * session the cassandra connection
    
    """
    # Define out cluster/session
    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()
    
    # If there is no sparkify keyspace, build one.
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
    
    # Set our keyspace to sparkify, which was just created.
    session.set_keyspace('sparkify')
    
    print('sparkify keyspace is created and set. \n')
    
    # Return our 'connection' variables
    return cluster, session


def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    """
    
    # Drop any pre-defined tables from the sql_queries 'library'
    for query in drop_table_queries:
        session.execute(drop_table_queries[query])
        print('{} table in the sparkify keyspace has been dropped.'.format(query[:-5]))
        
    print('\n')


def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    
    """
    
    # Create any pre-defined tables from the sql_queries 'library'
    for query in create_table_queries:
        session.execute(create_table_queries[query])
        print('{} table in the sparkify keyspace has been added.'.format(query[:-7]))
        
    print('\n')
        

def main():
    """
    main is the first function that runs subsequently run three
    consecutive functions to build our keyspace and tables. Once
    those are completed the session and cluster are shutdown.
    
    """

    
    cluster, session = create_keyspace()
    drop_tables(session)
    create_tables(session)
              
    
    session.shutdown()
    print('session shutdown')
    cluster.shutdown()
    print('cluster shutdown \n')
    
    print('Program \'create_tables\' has ended.')
    


if __name__ == "__main__":
    main()
       