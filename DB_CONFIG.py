
import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=os.getenv('P_host'),
            database=os.getenv('P_database'),
            user=os.getenv('P_user'),
            password=os.getenv('P_password'),
            port=os.getenv('P_port')
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise
