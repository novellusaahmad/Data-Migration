import streamlit as st
import io
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import logging
import mysql.connector
from mysql.connector import Error
import warnings
from typing import List, Optional

# Suppress warnings
warnings.filterwarnings('ignore')

# Initialize configuration variables
connection_string = ""
file_system_name = ""
directory_name = ""
mysql_host = ""
mysql_db = ""
mysql_user = ""
mysql_password = ""
create_tables = True
add_missing_columns = True

# Set up logging
log_file_path = 'data_pipeline.log'
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Streamlit app configuration
st.set_page_config(page_title="ADLS to MySQL Data Pipeline", layout="wide")
st.title("Azure Data Lake to MySQL Data Pipeline")
st.markdown("This application transfers data from Azure Data Lake Storage to MySQL Database.")

def setup_sidebar():
    """Configure the sidebar inputs."""
    global connection_string, file_system_name, directory_name
    global mysql_host, mysql_db, mysql_user, mysql_password
    global create_tables, add_missing_columns
    
    with st.sidebar:
        st.header("Configuration")
        connection_string = st.text_input(
            "ADLS Connection String",
            value="DefaultEndpointsProtocol=https;AccountName=novellusdatalake;AccountKey=xxAE3N5WEF109Opauf4ARZ5P4O36JUDJmDHk8gvWGc1KeZMcNDaEDNrShKs4nrDXAtcaIb0TQBiw+AStfYmrQA==;EndpointSuffix=core.windows.net",
            type="password"
        )
        file_system_name = st.text_input("File System Name", value="source")
        directory_name = st.text_input("Directory Path", value="/")
        
        st.subheader("MySQL Configuration")
        mysql_host = st.text_input("MySQL Host", value="novellus-db.mysql.database.azure.com")
        mysql_db = st.text_input("MySQL Database", value="legacy_data")
        mysql_user = st.text_input("MySQL Username", value="novellus")
        mysql_password = st.text_input("MySQL Password", value="BgGy597Bb1Q9ELQVZ6", type="password")
        
        st.subheader("Options")
        create_tables = st.checkbox("Create tables if they don't exist", value=True)
        add_missing_columns = st.checkbox("Add missing columns to existing tables", value=True)
        st.info("Note: For large tables, columns will be converted to TEXT to avoid size limits")

# File mapping dictionary
FILE_MAPPING = {
    'TABLE_ALLRATES.csv': 'rates',
    'TableMasterLoanSterling.csv': 'master_data',
    'Table_ExtensionDefault.csv': 'ext',
    'TableCashbookEuro.csv': 'tablecashbookeuro',
    'TableCashbookEuroNFL.csv': 'tablecashbookeuronfl',
    'TableCashbookItemType.csv': 'tableCashbookItemtype',
    'TableCashbookNCLEURO.csv': 'tableCashbookncleuro',
    'TableCashbookNCLSTERLING.csv': 'tablecashbooknclsterling',
    'TableCashbookSterling.csv': 'tablecashbooksterling',
    'TableTermLoan.csv': 'TableTermLoan',
    'TableVariableRateMapping.csv': 'TableVariableRateMapping',
    'TableBorrower.csv': 'TableBorrower',
    'TableBorrowerContact.csv': 'TableBorrowerContact',
    'TableCashbookNCLSTERLING_II.csv': 'TableCashbookNCLSTERLING_II',
    'TableCashbookNPLSTERLING.csv': 'TableCashbookNPLSTERLING',
    'TableLawyer.csv': 'TableLawyer',
    'Table_EIR.csv': 'Table_EIR'
}

def get_table_name(filename: str) -> str:
    """Map ADLS filename to MySQL table name."""
    return FILE_MAPPING.get(filename, "Invalid input")

def clean_column_name(col: str) -> str:
    """Clean and standardize column names for MySQL."""
    return (col.replace('#_', '')
               .replace(' ', '_')
               .replace('-', '_')
               .replace('/', '')
               .replace('_x0023__', '')
               .replace('%', 'Perc')
               .strip())

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare the DataFrame for MySQL insertion."""
    # Drop unwanted columns
    df.drop(columns=['@odata.etag', 'ItemInternalId', 'Errors'], inplace=True, errors='ignore')
    
    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Convert all data to string and handle nulls
    df = df.astype(str)
    df = df.where(pd.notnull(df), None)
    df = df.fillna('')
    
    # Replace invalid entries
    replacements = {
        'nan': '', 'NaT': '', '-': '', 
        '#REF!': '0', '#DIV/0!': '0'
    }
    df.replace(replacements, inplace=True)
    
    return df

def create_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Create MySQL connection with strict mode disabled."""
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            database=mysql_db,
            user=mysql_user,
            password=mysql_password,
            connection_timeout=30
        )
        if connection.is_connected():
            disable_strict_mode(connection)
            return connection
    except Error as e:
        st.error(f"Connection failed: {e}")
    return None

def disable_strict_mode(connection: mysql.connector.MySQLConnection) -> None:
    """Disable MySQL strict mode for current session."""
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute("SET SESSION sql_mode = ''")
        connection.commit()
    except Error as e:
        st.warning(f"Couldn't disable strict mode: {e}")
    finally:
        if cursor: 
            cursor.close()

def get_table_columns(connection: mysql.connector.MySQLConnection, table_name: str) -> List[str]:
    """Get list of columns in a table."""
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
        return [col[0] for col in cursor.fetchall()]
    except Error:
        return []
    finally:
        if cursor: 
            cursor.close()

def convert_columns_to_text(connection: mysql.connector.MySQLConnection, table_name: str) -> None:
    """Convert VARCHAR columns to TEXT to free up row space."""
    cursor = None
    try:
        disable_strict_mode(connection)
        cursor = connection.cursor()
        
        # Get column details
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{mysql_db}' 
            AND TABLE_NAME = '{table_name}'
            AND DATA_TYPE = 'varchar'
        """)
        varchar_columns = [col[0] for col in cursor.fetchall()]
        
        # Convert each VARCHAR column to TEXT
        for col in varchar_columns:
            try:
                cursor.execute(f"ALTER TABLE `{table_name}` MODIFY COLUMN `{col}` TEXT")
                st.info(f"Converted column '{col}' to TEXT to free space")
            except Error as e:
                st.warning(f"Couldn't convert column '{col}': {e}")
        
        connection.commit()
    except Error as e:
        st.error(f"Column conversion failed: {e}")
    finally:
        if cursor: 
            cursor.close()

def create_table_if_not_exists(connection: mysql.connector.MySQLConnection, 
                             table_name: str, 
                             df_columns: List[str]) -> None:
    """Create table with all TEXT columns to avoid size limits."""
    cursor = None
    try:
        disable_strict_mode(connection)
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        
        if not cursor.fetchone():
            # Create all columns as TEXT
            columns = [f"`{col}` TEXT" for col in df_columns]
            cursor.execute(f"""
                CREATE TABLE `{table_name}` (
                    {', '.join(columns)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            connection.commit()
            st.success(f"Created table '{table_name}' with {len(columns)} TEXT columns")
    except Error as e:
        st.error(f"Table creation failed: {e}")
    finally:
        if cursor: 
            cursor.close()

def add_missing_columns_to_table(connection: mysql.connector.MySQLConnection,
                               table_name: str,
                               df_columns: List[str]) -> None:
    """Add columns with automatic space management."""
    cursor = None
    try:
        disable_strict_mode(connection)
        cursor = connection.cursor()
        existing_columns = get_table_columns(connection, table_name)
        missing_columns = [col for col in df_columns if col not in existing_columns]
        
        if not missing_columns:
            return
            
        # Attempt to add columns
        for col in missing_columns:
            try:
                cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` TEXT")
                st.success(f"Added column '{col}' as TEXT")
            except Error as e:
                if "Row size too large" in str(e):
                    st.warning("Row size limit reached, converting columns to TEXT...")
                    convert_columns_to_text(connection, table_name)
                    # Retry after conversion
                    try:
                        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` TEXT")
                        st.success(f"Added column '{col}' after conversion")
                    except Error as e2:
                        st.error(f"Still can't add column '{col}': {e2}")
                else:
                    st.error(f"Couldn't add column '{col}': {e}")
        
        connection.commit()
    except Error as e:
        st.error(f"Column addition failed: {e}")
    finally:
        if cursor: 
            cursor.close()

def insert_data_from_df(df: pd.DataFrame, table_name: str) -> None:
    """Insert data with strict mode disabled and proper error handling."""
    connection = None
    cursor = None
    try:
        connection = create_connection()
        if not connection:
            return
            
        # Ensure strict mode is disabled
        disable_strict_mode(connection)
        cursor = connection.cursor()
        
        # Create table if needed
        if create_tables:
            create_table_if_not_exists(connection, table_name, df.columns)
            
        # Add missing columns
        if add_missing_columns:
            add_missing_columns_to_table(connection, table_name, df.columns)
        
        # Verify all columns exist
        table_columns = get_table_columns(connection, table_name)
        missing_in_table = [col for col in df.columns if col not in table_columns]
        
        if missing_in_table:
            st.error(f"Missing columns in table: {', '.join(missing_in_table)}")
            return
        
        # Prepare data
        clean_cols = [clean_column_name(col) for col in df.columns]
        placeholders = ', '.join(['%s'] * len(clean_cols))
        insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in clean_cols])}) VALUES ({placeholders})"
        
        # Execute insert
        data_to_insert = [tuple(row) for row in df.values]
        cursor.executemany(insert_sql, data_to_insert)
        connection.commit()
        st.success(f"Inserted {len(df)} rows into '{table_name}'")
        
    except Error as e:
        if connection: 
            connection.rollback()
        st.error(f"Insert failed: {e}")
    finally:
        if cursor: 
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def process_file(file_client, file_name: str) -> Optional[pd.DataFrame]:
    """Process a single file from ADLS."""
    try:
        st.info(f"Processing file: {file_name}")
        download = file_client.download_file()
        file_content = download.readall()
        file_stream = io.BytesIO(file_content)
        df = pd.read_csv(file_stream)
        return clean_dataframe(df)
    except Exception as e:
        st.error(f"Error processing {file_name}: {e}")
        return None

def main():
    """Main application function."""
    setup_sidebar()
    
    if st.button("Run Data Pipeline"):
        try:
            # Initialize ADLS client
            service_client = DataLakeServiceClient.from_connection_string(connection_string)
            file_system_client = service_client.get_file_system_client(file_system_name)
            directory_client = file_system_client.get_directory_client(directory_name)
            
            # Get list of files
            paths = directory_client.get_paths()
            file_list = [path.name for path in paths if not path.is_directory]
            
            if not file_list:
                st.warning("No files found in the specified directory.")
                return
            
            st.subheader("Files to Process")
            st.write(file_list)
            
            # Process each file
            for file_name in file_list:
                table_name = get_table_name(file_name)
                if table_name == "Invalid input":
                    st.warning(f"No table mapping found for file: {file_name}")
                    continue
                
                with st.expander(f"Processing {file_name} → {table_name}"):
                    file_client = directory_client.get_file_client(file_name)
                    df = process_file(file_client, file_name)
                    if df is not None:
                        st.dataframe(df.head())
                        st.write(f"Data shape: {df.shape}")
                        insert_data_from_df(df, table_name)
            
            st.success("Data pipeline completed successfully!")
            
        except Exception as e:
            st.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()