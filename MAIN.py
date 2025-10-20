import os
from EXTRACT import fetch_rentcast_data
from TRANSFORM import process_rentcast_data
from CREATE_TABLE import process_dimension_tables, create_database_tables
from LOAD import load_all_tables
from DB_CONFIG import get_db_connection
from typing import Callable

def main(zip_code: str = "78204", csv_dir: str = "cleaned_data", get_db_connection: Callable = None) -> None:
    """
    Main function to orchestrate the RentCast data pipeline.
    
    Args:
        zip_code: ZIP code to fetch property data for
        csv_dir: Directory to store CSV files
        get_db_connection: Function that returns a database connection
    """
    try:
        # Step 1: Fetch data
        print("Starting data fetching...")
        df_properties, df_sale_listings = fetch_rentcast_data(
            zip_code=zip_code,
            limit=50,
            max_addresses=10
        )
        
        # Step 2: Process data
        print("\nStarting data processing...")
        df_merged = process_rentcast_data(
            df_properties=df_properties,
            df_sale_listings=df_sale_listings,
            output_file=os.path.join(csv_dir, "property_data.csv")
        )
        
        # Step 3: Create dimension and fact tables
        print("\nStarting table creation...")
        tables = process_dimension_tables(
            df=df_merged,
            output_dir=csv_dir
        )
        
        # Step 4: Create database schema
        if get_db_connection is None:
            print("No database connection provided. Skipping database table creation and loading.")
            return
        
        print("\nCreating database schema...")
        create_database_tables(get_db_connection)
        
        # Step 5: Load data into database
        print("\nLoading data into database...")
        load_all_tables(
            csv_dir=csv_dir,
            get_db_connection=get_db_connection
        )
        
        print("\nPipeline completed successfully.")
        
    except Exception as e:
        print(f"Pipeline failed with error: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    # Note: get_db_connection must be defined or imported from another module
    # For example: from db_utils import get_db_connection
    csv_directory = r'C:\Users\back2\Desktop\primesqure_API_Project\src\cleaned_data'
    # main(zip_code="78204", csv_dir=csv_directory, get_db_connection=get_db_connection)
    pass