import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Optional, Tuple

def load_api_key() -> str:
    """Load API key from .env file."""
    load_dotenv()
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise RuntimeError("API_KEY not found in environment. Put API_KEY=... in your .env file")
    return api_key

def safe_get_json(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None, 
                 timeout: float = 20) -> Optional[Dict]:
    """
    Request JSON and return parsed JSON (or None on failure).
    Args:
        url: API endpoint URL
        params: Query parameters
        headers: Request headers
        timeout: Request timeout in seconds
    Returns:
        Parsed JSON response or None if request fails
    """
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        print(f"Request error for {url} with params {params}: {e}")
        return None

    if resp.status_code != 200:
        print(f"Non-200 response ({resp.status_code}) for {url} with params {params}: {resp.text[:200]}")
        return None

    try:
        return resp.json()
    except ValueError:
        print(f"Failed to parse JSON for {url} with params {params}")
        return None

def fetch_addresses(search_params: Dict, api_key: str, max_addresses: int = 10) -> List[str]:
    """
    Fetch unique addresses from sale listings.
    Args:
        search_params: Parameters for sale listing query (e.g., zipCode, limit)
        api_key: RentCast API key
        max_addresses: Maximum number of unique addresses to return
    Returns:
        List of unique addresses
    """
    BASE_SALE_URL = "https://api.rentcast.io/v1/listings/sale"
    headers = {"accept": "application/json", "x-api-key": api_key}
    
    print(f"\n=== Fetching sale listings for {search_params.get('zipCode', 'unknown')} ===")
    sale_listings = safe_get_json(BASE_SALE_URL, params=search_params, headers=headers)
    
    addresses = []
    if sale_listings and isinstance(sale_listings, list):
        for listing in sale_listings:
            address = listing.get("formattedAddress")
            if address and address not in addresses:
                addresses.append(address)
                if len(addresses) >= max_addresses:
                    break
        print(f" -> Found {len(addresses)} unique addresses")
    else:
        print(" -> No sale listings returned")
    
    return addresses

def fetch_property_data(addresses: List[str], api_key: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Fetch property and sale listing data for given addresses.
    Args:
        addresses: List of addresses to query
        api_key: RentCast API key
    Returns:
        Tuple of (property_records, sale_listing_records)
    """
    BASE_PROPERTY_URL = "https://api.rentcast.io/v1/properties"
    BASE_SALE_URL = "https://api.rentcast.io/v1/listings/sale"
    headers = {"accept": "application/json", "x-api-key": api_key}
    
    property_records = []
    sale_listing_records = []
    
    for address in addresses:
        print(f"\n=== Getting data for: {address} ===")
        params = {"address": address}
        
        # Fetch property data
        prop_json = safe_get_json(BASE_PROPERTY_URL, params=params, headers=headers)
        if prop_json and isinstance(prop_json, list) and len(prop_json) > 0:
            property_records.append(prop_json[0])
            print(" -> Property data appended")
        else:
            print(" -> No property data returned")
        
        # Fetch sale listing data
        sale_json = safe_get_json(BASE_SALE_URL, params=params, headers=headers)
        if sale_json and isinstance(sale_json, list) and len(sale_json) > 0:
            sale_listing_records.append(sale_json[0])
            print(" -> Sale listing appended")
        else:
            print(" -> No sale listing returned")
        
        time.sleep(0.2)  # API rate limiting
    
    return property_records, sale_listing_records

def fetch_rentcast_data(zip_code: str, limit: int = 50, max_addresses: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main function to fetch and process RentCast property data.
    Args:
        zip_code: ZIP code to search for properties
        limit: Maximum number of sale listings to fetch
        max_addresses: Maximum number of unique addresses to process
    Returns:
        Tuple of (properties_df, sale_listings_df) DataFrames
    """
    # Initialize
    api_key = load_api_key()
    search_params = {"zipCode": zip_code, "limit": limit}
    
    # Fetch addresses
    addresses = fetch_addresses(search_params, api_key, max_addresses)
    
    # Fetch data for addresses
    property_records, sale_listing_records = fetch_property_data(addresses, api_key)
    
    # Convert to DataFrames
    df_properties = pd.json_normalize(property_records) if property_records else pd.DataFrame()
    df_sale_listings = pd.json_normalize(sale_listing_records) if sale_listing_records else pd.DataFrame()
    
    print("\n✅ Data extraction complete.")
    print("Properties rows:", len(df_properties))
    print("Sale listings rows:", len(df_sale_listings))
    
    return df_properties, df_sale_listings

if __name__ == "__main__":
    # Example usage
    df_properties, df_sale_listings = fetch_rentcast_data(zip_code="78204")