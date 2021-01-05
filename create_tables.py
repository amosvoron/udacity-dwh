import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, table_list_query


def drop_tables(cur, conn):
    """Drops tables from DWH.
    
    Args:
        cur: The PostgreSQL cursor object. 
        conn: The connection object.
        
    Returns:
        None
    """           
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates tables in DWH.
    
    Args:
        cur: The PostgreSQL cursor object. 
        conn: The connection object.
        
    Returns:
        None
    """           
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}" .format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    print('Table list:')
    print('--------------------------------------------')
    cur.execute(table_list_query)
    for row in cur.fetchall():
        print(row)  

    print('\nTables are successfully created.\n')
    
    conn.close()


if __name__ == "__main__":
    main()