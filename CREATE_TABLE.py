import os
import pandas as pd
from typing import Tuple

def create_output_directory(base_path: str = "cleaned_data") -> str:
    """
    Create output directory if it doesn't exist.
    
    Args:
        base_path: Directory path to create
        
    Returns:
        Absolute path of the created directory
    """
    os.makedirs(base_path, exist_ok=True)
    return os.path.abspath(base_path)

def create_location_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create location dimension table from input DataFrame.
    
    Args:
        df: Input DataFrame containing property data
        
    Returns:
        Location dimension DataFrame with location_id
    """
    location_dim = df[['city', 'state', 'zip_Code', 'county', 'longitude', 'latitude']]\
        .copy().drop_duplicates().reset_index(drop=True)
    location_dim.index.name = 'location_id'
    location_dim = location_dim.reset_index()
    return location_dim

def create_property_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create property dimension table from input DataFrame.
    
    Args:
        df: Input DataFrame containing property data
        
    Returns:
        Property dimension DataFrame with property_id
    """
    property_dim = df[['property_code', 'property_Address', 'property_Type',
                      'bedrooms', 'bathrooms', 'square_Footage', 'year_Built',
                      'lot_Size']].copy().drop_duplicates().reset_index(drop=True)
    property_dim.index.name = 'property_id'
    property_dim = property_dim.reset_index()
    return property_dim

def create_agent_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create agent dimension table from input DataFrame.
    
    Args:
        df: Input DataFrame containing agent data
        
    Returns:
        Agent dimension DataFrame with agent_id
    """
    agent_dim = df[['agent_name', 'agent_phone', 'agent_email']]\
        .copy().drop_duplicates().reset_index(drop=True)
    agent_dim.index.name = 'agent_id'
    agent_dim = agent_dim.reset_index()
    return agent_dim

def create_owner_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create owner dimension table from input DataFrame, converting owner_names lists to strings.
    
    Args:
        df: Input DataFrame containing owner data
        
    Returns:
        Owner dimension DataFrame with owner_id
    """
    df_copy = df.copy()
    df_copy['owner_names'] = df_copy['owner_names'].apply(
        lambda x: ', '.join(x) if isinstance(x, list) else x
    )
    owner_dim = df_copy[['owner_names', 'owner_type', 'owner_Occupied']]\
        .copy().drop_duplicates().reset_index(drop=True)
    owner_dim.index.name = 'owner_id'
    owner_dim = owner_dim.reset_index()
    return owner_dim

def create_office_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create office dimension table from input DataFrame.
    
    Args:
        df: Input DataFrame containing office data
        
    Returns:
        Office dimension DataFrame with office_id
    """
    office_dim = df[['listing_office_name', 'listing_office_phone',
                    'listing_office_email', 'listing_office_website']]\
        .copy().drop_duplicates().reset_index(drop=True)
    office_dim.index.name = 'office_id'
    office_dim = office_dim.reset_index()
    return office_dim

def create_listing_dim_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create listing dimension table from input DataFrame.
    
    Args:
        df: Input DataFrame containing listing data
        
    Returns:
        Listing dimension DataFrame with listing_id
    """
    listing_dim = df[['listing_code', 'listing_Type']]\
        .copy().drop_duplicates().reset_index(drop=True)
    listing_dim.index.name = 'listing_id'
    listing_dim = listing_dim.reset_index()
    return listing_dim

def create_fact_table(df: pd.DataFrame, dimension_tables: Tuple[pd.DataFrame, ...]) -> pd.DataFrame:
    """
    Create fact table by merging input DataFrame with dimension tables.
    
    Args:
        df: Input DataFrame containing property data
        dimension_tables: Tuple of (property_dim, owner_dim, location_dim, agent_dim, office_dim, listing_dim)
        
    Returns:
        Fact table DataFrame with fact_id
    """
    property_dim, owner_dim, location_dim, agent_dim, office_dim, listing_dim = dimension_tables
    
    df_clean = df.drop(columns=['_merge'], errors='ignore').copy()
    
    fact_table = (df_clean.merge(
        property_dim,
        on=['property_code', 'property_Address', 'property_Type', 'bedrooms',
            'bathrooms', 'square_Footage', 'year_Built', 'lot_Size'],
        how='left')
        .merge(owner_dim, on=['owner_names', 'owner_type', 'owner_Occupied'], how='left')
        .merge(location_dim, on=['city', 'state', 'zip_Code', 'county', 'longitude', 'latitude'], how='left')
        .merge(agent_dim, on=['agent_name', 'agent_phone', 'agent_email'], how='left')
        .merge(office_dim, on=['listing_office_name', 'listing_office_phone',
                             'listing_office_email', 'listing_office_website'], how='left')
        .merge(listing_dim, on=['listing_code', 'listing_Type'], how='left'))
    
    selected_columns = [
        'property_id', 'owner_id', 'location_id', 'agent_id', 'office_id', 'listing_id',
        'status', 'price', 'listing_Type', 'listed_Date', 'last_saleDate',
        'removed_Date', 'created_Date', 'last_Seen_Date', 'property_Type', 'last_SalePrice'
    ]
    
    available_columns = [col for col in selected_columns if col in fact_table.columns]
    fact_table = fact_table[available_columns].copy()
    
    if 'fact_id' not in fact_table.columns:
        fact_table['fact_id'] = fact_table.index + 1
    
    fact_table = fact_table[['fact_id'] + [col for col in available_columns if col != 'fact_id']]\
        .drop_duplicates().reset_index(drop=True)
    fact_table.index.name = 'fact_id'
    
    return fact_table

def save_dimension_tables(tables: Tuple[pd.DataFrame, ...], output_dir: str = "cleaned_data") -> None:
    """
    Save dimension and fact tables to CSV files.
    
    Args:
        tables: Tuple of (location_dim, property_dim, agent_dim, owner_dim, office_dim, listing_dim, fact_table)
        output_dir: Directory to save CSV files
    """
    table_names = [
        'location_dim.csv', 'property_dim_table.csv', 'agent_data.csv',
        'owner_data.csv', 'office_data.csv', 'listing_data.csv', 'fact_data.csv'
    ]
    
    for table, name in zip(tables, table_names):
        table.to_csv(os.path.join(output_dir, name), index=False)

def create_database_tables(get_db_connection) -> None:
    """
    Create database schema and tables for the primesquare database.
    
    Args:
        get_db_connection: Function that returns a database connection
    """
    create_table_query = '''
        CREATE SCHEMA IF NOT EXISTS primesquare;

        DROP TABLE IF EXISTS primesquare.property_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.owner_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.location_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.agent_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.office_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.listing_dim_table CASCADE;
        DROP TABLE IF EXISTS primesquare.fact_dim_table CASCADE;

        CREATE TABLE IF NOT EXISTS primesquare.property_dim_table (
            property_id SERIAL PRIMARY KEY,
            property_code VARCHAR(50) NOT NULL,
            property_address VARCHAR(255) NOT NULL,
            property_type VARCHAR(50),
            bedrooms INTEGER,
            bathrooms INTEGER,
            square_footage INTEGER,
            year_built INTEGER,
            lot_size INTEGER,
            UNIQUE (property_code, property_address, property_type, bedrooms, bathrooms, square_footage, year_built, lot_size)
        );

        CREATE TABLE IF NOT EXISTS primesquare.owner_dim_table (
            owner_id SERIAL PRIMARY KEY,
            owner_names VARCHAR(500) NOT NULL,
            owner_type VARCHAR(100),
            owner_occupied BOOLEAN,
            UNIQUE (owner_names, owner_type, owner_occupied)
        );

        CREATE TABLE IF NOT EXISTS primesquare.location_dim_table (
            location_id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            state VARCHAR(50),
            zip_code VARCHAR(20),
            county VARCHAR(100),
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            UNIQUE (city, state, zip_code, county, longitude, latitude)
        );

        CREATE TABLE IF NOT EXISTS primesquare.agent_dim_table (
            agent_id SERIAL PRIMARY KEY,
            agent_name VARCHAR(255) NOT NULL,
            agent_phone VARCHAR(20),
            agent_email VARCHAR(255),
            UNIQUE (agent_name, agent_phone, agent_email)
        );

        CREATE TABLE IF NOT EXISTS primesquare.office_dim_table (
            office_id SERIAL PRIMARY KEY,
            listing_office_name VARCHAR(255) NOT NULL,
            listing_office_phone VARCHAR(20),
            listing_office_email VARCHAR(255),
            listing_office_website VARCHAR(255),
            UNIQUE (listing_office_name, listing_office_phone, listing_office_email, listing_office_website)
        );

        CREATE TABLE IF NOT EXISTS primesquare.listing_dim_table (
            listing_id SERIAL PRIMARY KEY,
            listing_code VARCHAR(50) NOT NULL,
            listing_type VARCHAR(50),
            UNIQUE (listing_code, listing_type)
        );

        CREATE TABLE IF NOT EXISTS primesquare.fact_dim_table (
            fact_id SERIAL PRIMARY KEY,
            property_id INTEGER NOT NULL,
            owner_id INTEGER,
            location_id INTEGER,
            agent_id INTEGER,
            office_id INTEGER,
            listing_id INTEGER,
            status VARCHAR(50),
            price DECIMAL(15, 2),
            listing_type VARCHAR(50),
            listed_date TIMESTAMP,
            last_saleDate TIMESTAMP,
            removed_date TIMESTAMP,
            created_date TIMESTAMP,
            last_seen_date TIMESTAMP,
            property_Type VARCHAR(100),
            last_sale_price DECIMAL(15, 2),
            FOREIGN KEY (property_id) REFERENCES primesquare.property_dim_table (property_id),
            FOREIGN KEY (owner_id) REFERENCES primesquare.owner_dim_table (owner_id),
            FOREIGN KEY (location_id) REFERENCES primesquare.location_dim_table (location_id),
            FOREIGN KEY (agent_id) REFERENCES primesquare.agent_dim_table (agent_id),
            FOREIGN KEY (office_id) REFERENCES primesquare.office_dim_table (office_id),
            FOREIGN KEY (listing_id) REFERENCES primesquare.listing_dim_table (listing_id)
        );

        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_property_id 
            ON primesquare.fact_dim_table (property_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_owner_id 
            ON primesquare.fact_dim_table (owner_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_location_id 
            ON primesquare.fact_dim_table (location_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_agent_id 
            ON primesquare.fact_dim_table (agent_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_office_id 
            ON primesquare.fact_dim_table (office_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_listing_id 
            ON primesquare.fact_dim_table (listing_id);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_status 
            ON primesquare.fact_dim_table (status);
        CREATE INDEX IF NOT EXISTS idx_fact_primesquare_listed_date 
            ON primesquare.fact_dim_table (listed_date);
    '''
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def process_dimension_tables(df: pd.DataFrame, output_dir: str = "cleaned_data") -> Tuple[pd.DataFrame, ...]:
    """
    Main function to process dimension and fact tables from input DataFrame.
    
    Args:
        df: Input DataFrame containing property data
        output_dir: Directory to save CSV files
    
    Returns:
        Tuple of (location_dim, property_dim, agent_dim, owner_dim, office_dim, listing_dim, fact_table)
    """
    output_path = create_output_directory(output_dir)
    print(f"CSV files will be created at: {output_path}")
    
    # Create dimension tables
    location_dim = create_location_dim_table(df)
    property_dim = create_property_dim_table(df)
    agent_dim = create_agent_dim_table(df)
    owner_dim = create_owner_dim_table(df)
    office_dim = create_office_dim_table(df)
    listing_dim = create_listing_dim_table(df)
    
    # Create fact table
    fact_table = create_fact_table(df, (property_dim, owner_dim, location_dim, 
                                      agent_dim, office_dim, listing_dim))
    
    # Save all tables
    save_dimension_tables((location_dim, property_dim, agent_dim, owner_dim, 
                         office_dim, listing_dim, fact_table), output_dir)
    
    return location_dim, property_dim, agent_dim, owner_dim, office_dim, listing_dim, fact_table

if __name__ == "__main__":
    # Example usage (assuming df_primesquare is available)
    # df_primesquare = pd.read_csv("property_data.csv")
    # tables = process_dimension_tables(df_primesquare)
    # create_database_tables(get_db_connection)  # Assumes get_db_connection is defined
    pass