import os
import csv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Callable, Optional

# Type alias: get_db_connection should be a callable that returns a psycopg2 connection
GetDBConnection = Callable[[], psycopg2.extensions.connection]

def _safe_close(conn: Optional[psycopg2.extensions.connection],
                cursor: Optional[psycopg2.extensions.cursor]) -> None:
    try:
        if cursor is not None:
            cursor.close()
    except Exception:
        pass
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass

def _to_py_none(val):
    # Convert pandas NA / numpy nan to None for psycopg2
    if pd.isna(val):
        return None
    return val

def load_property_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        with open(csv_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader, None)  # Skip header safely
            records = []
            for row in reader:
                if not row or all([cell.strip() == "" for cell in row]):
                    continue
                try:
                    records.append((
                        int(row[0]),                      # property_id
                        row[1] or None,                   # property_code
                        row[2] or None,                   # property_address
                        row[3] or None,                   # property_type
                        int(float(row[4])) if row[4] else None,  # bedrooms
                        int(float(row[5])) if row[5] else None,  # bathrooms
                        int(float(row[6])) if row[6] else None,  # square_footage
                        int(float(row[7])) if row[7] else None,  # year_built
                        float(row[8]) if row[8] else None         # lot_size
                    ))
                except Exception as e:
                    # Skip malformed rows but print a warning
                    print(f"Skipping row due to parse error: {row} -> {e}")

        if not records:
            print("No valid records found to insert into property_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.property_dim_table(
                property_id, property_code, property_address, property_type,
                bedrooms, bathrooms, square_footage, year_built, lot_size
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into property_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_owner_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)  # read everything as object to avoid surprises
        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            try:
                occ = row.get('owner_Occupied', None)
                if isinstance(occ, str):
                    occ_val = occ.strip().lower() in ['1', 'true', 'yes']
                else:
                    occ_val = bool(occ) if not pd.isna(occ) else None

                records.append((
                    int(row['owner_id']) if not pd.isna(row['owner_id']) else None,
                    row.get('owner_names', None),
                    row.get('owner_type', None),
                    occ_val
                ))
            except Exception as e:
                print(f"Skipping owner row due to parse error: {row} -> {e}")

        if not records:
            print("No valid records found to insert into owner_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.owner_dim_table(
                owner_id, owner_names, owner_type, owner_occupied
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into owner_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_location_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)

        expected_columns = ['location_id', 'city', 'state', 'zip_Code', 'county', 'longitude', 'latitude']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in CSV: {missing_columns}")

        # Normalize and convert
        df['location_id'] = pd.to_numeric(df['location_id'], errors='coerce').fillna(0).astype(int)
        df['zip_Code'] = df['zip_Code'].astype(str).where(~df['zip_Code'].isna(), None)
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').where(lambda s: ~s.isna(), None)
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').where(lambda s: ~s.isna(), None)

        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            records.append((
                int(row['location_id']),
                row.get('city', None),
                row.get('state', None),
                row.get('zip_Code', None),
                row.get('county', None),
                None if pd.isna(row.get('longitude')) else float(row.get('longitude')),
                None if pd.isna(row.get('latitude')) else float(row.get('latitude'))
            ))

        if not records:
            print("No valid records found to insert into location_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.location_dim_table(
                location_id, city, state, zip_code, county, longitude, latitude
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into location_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_agent_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)
        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            try:
                records.append((
                    int(row['agent_id']) if not pd.isna(row['agent_id']) else None,
                    row.get('agent_name', None),
                    row.get('agent_phone', None),
                    row.get('agent_email', None)
                ))
            except Exception as e:
                print(f"Skipping agent row due to parse error: {row} -> {e}")

        if not records:
            print("No valid records found to insert into agent_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.agent_dim_table(
                agent_id, agent_name, agent_phone, agent_email
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into agent_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_office_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)
        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            try:
                records.append((
                    int(row['office_id']) if not pd.isna(row['office_id']) else None,
                    row.get('listing_office_name', None),
                    row.get('listing_office_phone', None),
                    row.get('listing_office_email', None),
                    row.get('listing_office_website', None)
                ))
            except Exception as e:
                print(f"Skipping office row due to parse error: {row} -> {e}")

        if not records:
            print("No valid records found to insert into office_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.office_dim_table(
                office_id, listing_office_name, listing_office_phone, listing_office_email, listing_office_website
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into office_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_listing_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)
        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            try:
                records.append((
                    int(row['listing_id']) if not pd.isna(row['listing_id']) else None,
                    row.get('listing_code', None),
                    row.get('listing_Type', None)
                ))
            except Exception as e:
                print(f"Skipping listing row due to parse error: {row} -> {e}")

        if not records:
            print("No valid records found to insert into listing_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.listing_dim_table(
                listing_id, listing_code, listing_type
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into listing_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_fact_dim_table(csv_path: str, get_db_connection: GetDBConnection) -> None:
    conn = None
    cursor = None
    try:
        df = pd.read_csv(csv_path, dtype=object)

        expected_columns = [
            'fact_id', 'property_id', 'owner_id', 'location_id', 'agent_id', 'office_id', 'listing_id',
            'status', 'price', 'listing_Type', 'listed_Date', 'last_saleDate', 'removed_Date',
            'created_Date', 'last_Seen_Date', 'last_SalePrice', 'property_Type'
        ]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns in CSV: {missing_columns}")

        # numeric conversions
        for col in ['fact_id', 'property_id', 'owner_id', 'location_id', 'agent_id', 'office_id', 'listing_id']:
            df[col] = pd.to_numeric(df[col], errors='coerce').where(lambda s: ~s.isna(), None)

        df['price'] = pd.to_numeric(df['price'], errors='coerce').where(lambda s: ~s.isna(), None)
        df['last_SalePrice'] = pd.to_numeric(df['last_SalePrice'], errors='coerce').where(lambda s: ~s.isna(), None)

        date_columns = ['listed_Date', 'last_saleDate', 'removed_Date', 'created_Date', 'last_Seen_Date']
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            df[col] = df[col].where(df[col].notnull(), None)

        conn = get_db_connection()
        cursor = conn.cursor()

        records = []
        for _, row in df.iterrows():
            records.append((
                None if pd.isna(row.get('fact_id')) else int(row.get('fact_id')),
                None if pd.isna(row.get('property_id')) else int(row.get('property_id')),
                None if pd.isna(row.get('owner_id')) else int(row.get('owner_id')),
                None if pd.isna(row.get('location_id')) else int(row.get('location_id')),
                None if pd.isna(row.get('agent_id')) else int(row.get('agent_id')),
                None if pd.isna(row.get('office_id')) else int(row.get('office_id')),
                None if pd.isna(row.get('listing_id')) else int(row.get('listing_id')),
                row.get('status', None),
                None if pd.isna(row.get('price')) else float(row.get('price')),
                row.get('listing_Type', None),
                row.get('listed_Date', None),
                row.get('last_saleDate', None),
                row.get('removed_Date', None),
                row.get('created_Date', None),
                row.get('last_Seen_Date', None),
                None if pd.isna(row.get('last_SalePrice')) else float(row.get('last_SalePrice')),
                row.get('property_Type', None)
            ))

        if not records:
            print("No valid records found to insert into fact_dim_table.")
            return

        execute_values(
            cursor,
            '''
            INSERT INTO primesquare.fact_dim_table(
                fact_id, property_id, owner_id, location_id, agent_id, office_id, listing_id,
                status, price, listing_type, listed_date, last_saledate, removed_date,
                created_date, last_seen_date, last_sale_price, property_type
            ) VALUES %s
            ''',
            records
        )

        conn.commit()
        print(f"Successfully loaded {len(records)} rows into fact_dim_table.")

    except FileNotFoundError:
        print(f"CSV file not found: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        print(f"CSV file is empty: {csv_path}")
        raise
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn is not None:
            conn.rollback()
        raise
    except Exception as e:
        print(f"Error processing CSV: {e}")
        raise
    finally:
        _safe_close(conn, cursor)


def load_all_tables(csv_dir: str, get_db_connection: GetDBConnection) -> None:
    table_configs = [
        ('property_dim_table.csv', load_property_dim_table),
        ('owner_data.csv', load_owner_dim_table),
        ('location_dim.csv', load_location_dim_table),
        ('agent_data.csv', load_agent_dim_table),
        ('office_data.csv', load_office_dim_table),
        ('listing_data.csv', load_listing_dim_table),
        ('fact_data.csv', load_fact_dim_table)
    ]

    for file_name, load_func in table_configs:
        csv_path = os.path.join(csv_dir, file_name)
        print(f"Loading {file_name}...")
        load_func(csv_path, get_db_connection)


if __name__ == "__main__":
    # Provide your get_db_connection function here when running.
    csv_dir = r'C:\Users\back2\Desktop\primesqure_API_Project\src\cleaned_data'
    # Example: load_all_tables(csv_dir, get_db_connection)
    pass
