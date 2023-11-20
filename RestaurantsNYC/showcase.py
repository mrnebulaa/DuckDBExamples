import duckdb
import pandas as pd
import os

def load_data(file_path, table_name, database_path):
    """
    Load data from a CSV file into a DuckDB table.
    
    Parameters:
    - file_path: The path to the CSV file.
    - table_name: The name of the DuckDB table to create.
    - database_path: The path to results
    
    Returns:
    - A DuckDB connection object.
    """
    database_file_path = f'{database_path}/{table_name}.db'

    if os.path.exists(database_file_path):
        con = duckdb.connect(database=database_file_path, read_only=False)
        
        # Check if the table already exists in the database
        existing_tables = con.execute('SHOW TABLES').fetchdf()['name'].tolist()
        if table_name in existing_tables:
            print(f"Table '{table_name}' already exists in the database.")
            return con
    else:
        # Create a new database if it doesn't exist
        os.makedirs(database_path, exist_ok=True)
        con = duckdb.connect(database=database_file_path, read_only=False)
    
    con.execute(f'CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(\'{file_path}\')')
    return con

def top_cuisines_by_borough(con):
    """
    Calculate and display the top 10 cuisines with the most violations in each borough using DuckDB.
    
    Parameters:
    - con: The DuckDB connection object.
    """
    # Get the list of unique boroughs
    result = con.execute('SELECT DISTINCT "BORO" FROM restaurant_data').fetchdf()
    boroughs = result['BORO'].tolist()

    for borough in boroughs:
        print(f"\nTop 10 Cuisines with Most Violations in {borough}:")
        
        # Fetch data for the specific borough
        query = f'''
            SELECT "CUISINE DESCRIPTION", COUNT("VIOLATION DESCRIPTION") AS "VIOLATION COUNT"
            FROM restaurant_data
            WHERE "BORO" = '{borough}'
            GROUP BY "CUISINE DESCRIPTION"
            ORDER BY "VIOLATION COUNT" DESC
            LIMIT 10
        '''
        result = con.execute(query).fetchdf()
        
        # Display the top 10 cuisines
        print(result)


if __name__ == "__main__":
    data_path = 'RestaurantsNYC\data\DOHMH_New_York_City_Restaurant_Inspection_Results.csv'
    table_name = 'restaurant_data'
    database_path = 'results'
    
    # Load data into DuckDB
    duckdb_connection = load_data(data_path, table_name, database_path)
    
    # Analyze and display top cuisines by borough
    top_cuisines_by_borough(duckdb_connection)
    
    # Close the DuckDB connection
    duckdb_connection.close()
