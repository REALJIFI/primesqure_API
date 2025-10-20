import pandas as pd
from typing import Tuple


def select_columns(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Select specific columns from properties and sale listings DataFrames.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
    
    Returns:
        Tuple of (filtered properties DataFrame, filtered sale listings DataFrame)
    """
    property_columns = [
        'id', 'formattedAddress', 'county', 'lastSaleDate', 'owner.names', 'owner.type',
        'lastSalePrice', 'ownerOccupied', 'bedrooms', 'bathrooms', 'squareFootage', 
        'yearBuilt', 'city', 'state', 'zipCode', 'latitude', 'longitude', 'propertyType', 
        'lotSize'
    ]
    
    sale_columns = [
        'id', 'status', 'price', 'listingType', 'listedDate', 'propertyType',
        'listingAgent.name', 'listingAgent.phone', 'listingAgent.email',
        'listingOffice.name', 'listingOffice.phone', 'listingOffice.email', 
        'listingOffice.website', 'removedDate', 'createdDate', 'lastSeenDate'
    ]
    
    return (
        df_properties[property_columns].copy(),
        df_sale_listings[sale_columns].copy()
    )

def rename_columns(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rename columns in properties and sale listings DataFrames for consistency.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
    
    Returns:
        Tuple of (renamed properties DataFrame, renamed sale listings DataFrame)
    """
    property_rename = {
        "id": "property_code",
        "propertyType": "property_Type",
        "owner.names": "owner_names",
        "owner.type": "owner_type",
        "lastSaleDate": "last_saleDate",
        "lastSalePrice": "last_SalePrice",
        "ownerOccupied": "owner_Occupied",
        "squareFootage": "square_Footage",
        "yearBuilt": "year_Built",
        "zipCode": "zip_Code",
        "lotSize": "lot_Size",
        "formattedAddress": "property_Address"
    }
    
    sale_rename = {
        "id": "listing_code",
        "listedDate": "listed_Date",
        "listingType": "listing_Type",
        "propertyType": "listing_property_Type",
        "listingAgent.name": "agent_name",
        "listingAgent.phone": "agent_phone",
        "listingAgent.email": "agent_email",
        "listingOffice.name": "listing_office_name",
        "listingOffice.phone": "listing_office_phone",
        "listingOffice.email": "listing_office_email",
        "listingOffice.website": "listing_office_website",
        "removedDate": "removed_Date",
        "createdDate": "created_Date",
        "lastSeenDate": "last_Seen_Date"
    }
    
    return (
        df_properties.rename(columns=property_rename),
        df_sale_listings.rename(columns=sale_rename)
    )

def convert_dates(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert date columns to datetime format.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
    
    Returns:
        Tuple of (properties DataFrame with converted dates, sale listings DataFrame with converted dates)
    """
    df_properties = df_properties.copy()
    df_sale_listings = df_sale_listings.copy()
    
    # Convert property date
    df_properties['last_saleDate'] = pd.to_datetime(df_properties['last_saleDate'], errors='coerce')
    
    # Convert sale listing dates
    date_cols = ['listed_Date', 'removed_Date', 'created_Date', 'last_Seen_Date']
    df_sale_listings[date_cols] = df_sale_listings[date_cols].apply(pd.to_datetime, errors='coerce')
    
    return df_properties, df_sale_listings

def fill_missing_values(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fill missing values in properties and sale listings DataFrames with specified defaults.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
    
    Returns:
        Tuple of (properties DataFrame with filled values, sale listings DataFrame with filled values)
    """
    df_properties = df_properties.copy()
    df_sale_listings = df_sale_listings.copy()
    
    # Fill rules for properties
    df_properties.fillna({
        'bathrooms': 0.0,
        'bedrooms': 0.0,
        'square_Footage': 0.0,
        'county': 'unknown',
        'property_Type': 'unknown',
        'year_Built': 0.0,
        'lot_Size': 0.0,
        'owner_Occupied': 0.0,
        'last_SalePrice': 0.0,
        'owner_names': 'unknown',
        'owner_type': 'unknown',
        'property_Address': 'unknown',
        'city': 'unknown',
        'state': 'unknown',
        'zip_Code': 'unknown',
        'latitude': 0.0,
        'longitude': 0.0,
        'last_saleDate': pd.NaT
    }, inplace=True)
    
    # Fill rules for sale listings
    df_sale_listings.fillna({
        'status': 'unknown',
        'price': 0.0,
        'listing_Type': 'unknown',
        'listed_Date': pd.NaT,
        'listing_property_Type': 'unknown',
        'agent_name': 'unknown',
        'agent_phone': 'unknown',
        'agent_email': 'unknown',
        'listing_office_name': 'unknown',
        'listing_office_phone': 'unknown',
        'listing_office_email': 'unknown',
        'listing_office_website': 'unknown',
        'removed_Date': pd.NaT,
        'created_Date': pd.NaT,
        'last_Seen_Date': pd.NaT
    }, inplace=True)
    
    return df_properties, df_sale_listings

def merge_dataframes(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame) -> pd.DataFrame:
    """
    Merge properties and sale listings DataFrames, removing duplicate columns.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
    
    Returns:
        Merged DataFrame
    """
    df_merged = pd.merge(
        df_properties,
        df_sale_listings,
        left_on='property_code',
        right_on='listing_code',
        how='left',
        suffixes=('', '_listing'),
        indicator=True
    )
    
    # Drop duplicate column
    df_merged.drop(['listing_property_Type'], axis=1, inplace=True)
    
    print("Merged shape:", df_merged.shape)
    return df_merged

def save_to_csv(df: pd.DataFrame, filename: str = "property_data.csv") -> None:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        filename: Output CSV filename
    """
    df.to_csv(filename, index=False)
    print(f"\nFiles saved: {filename}")

def read_from_csv(filename: str = "property_data.csv") -> pd.DataFrame:
    """
    Read DataFrame from CSV file.
    
    Args:
        filename: Input CSV filename
    
    Returns:
        DataFrame loaded from CSV
    """
    return pd.read_csv(filename)

def process_rentcast_data(df_properties: pd.DataFrame, df_sale_listings: pd.DataFrame, 
                         output_file: str = "property_data.csv") -> pd.DataFrame:
    """
    Main function to process RentCast data through column selection, renaming, date conversion,
    missing value handling, merging, and saving.
    
    Args:
        df_properties: DataFrame containing property data
        df_sale_listings: DataFrame containing sale listings data
        output_file: Output CSV filename
    
    Returns:
        Processed and merged DataFrame
    """
    # Select columns
    df_properties, df_sale_listings = select_columns(df_properties, df_sale_listings)
    
    # Rename columns
    df_properties, df_sale_listings = rename_columns(df_properties, df_sale_listings)
    
    # Convert dates
    df_properties, df_sale_listings = convert_dates(df_properties, df_sale_listings)
    
    # Fill missing values
    df_properties, df_sale_listings = fill_missing_values(df_properties, df_sale_listings)
    
    # Merge DataFrames
    df_merged = merge_dataframes(df_properties, df_sale_listings)
    
    # Save to CSV
    save_to_csv(df_merged, output_file)
    
    return df_merged

if __name__ == "__main__":
    # Example usage (assuming df_properties and df_sale_listings are available)
    # df_properties, df_sale_listings = ... (from previous data fetching)
    # df_processed = process_rentcast_data(df_properties, df_sale_listings)
    # df_primesquare = read_from_csv("property_data.csv")
    pass