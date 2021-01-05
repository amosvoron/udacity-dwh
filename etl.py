import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Extracts songs and log events data from the S3 data source
    and inserts it into the staging tables.
    
    Args:
        cur: The PostgreSQL cursor object. 
        conn: The connection object.
        
    Returns:
        None
    """    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Transforms data from the staging tables and inserts it into the analytics tables.
    
    Args:
        cur: The PostgreSQL cursor object. 
        conn: The connection object.
        
    Returns:
        None
    """        
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    print('Loading staging tables...')
    load_staging_tables(cur, conn)
    print('Staging tables done.')
    print('Inserting into analytics tables...')
    insert_tables(cur, conn)
    print('Analytics tables done.')
    
    conn.close()


if __name__ == "__main__":
    main()