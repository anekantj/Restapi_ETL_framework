"""
Generic OAuth2 API Data Integration Pipeline
Demonstrates: OAuth2 authentication, pagination, token refresh, data transformation, error handling
Use case: Fetching data from any OData-compliant REST API
"""

import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
import sys

load_dotenv()

# API Configuration - loaded from environment variables
BASE_URL = os.getenv("API_BASE_URL")
TOKEN_URL = os.getenv("TOKEN_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
GRANT_TYPE = os.getenv("GRANT_TYPE", "client_credentials")
SCOPE = os.getenv("SCOPE", "")

# Storing dataframes for each endpoint
dataframes = {}

# Example configuration for API endpoints
# In production, this would be loaded from a config file
ENDPOINT_CONFIGS = {
    "users": {
        "select": ["id", "username", "email", "created_date", "status"],
        "filter": "status eq 'active'",
        "key_filter": None
    },
    "orders": {
        "select": ["order_id", "user_id", "total_amount", "order_date", "status"],
        "filter": "",
        "key_filter": "user_id",
        "driving_view_col_name": "id"
    },
    "products": {
        "select": ["product_id", "name", "category", "price", "in_stock"],
        "filter": "",
        "key_filter": None
    }
}

# Example join operations for building final dataset
JOIN_OPERATIONS = [
    ("RENAME", "id", "user_id"),
    ("CAST", "user_id", "string"),
    ("JOIN", "df_orders", "user_id", "user_id"),
    ("CAST", "total_amount", "float64")
]

# Column mapping for final output
COLUMN_MAPPINGS = [
    ("user_id", "User ID"),
    ("username", "Customer Name"),
    ("email", "Email Address"),
    ("order_id", "Order Number"),
    ("total_amount", "Order Total"),
    ("order_date", "Purchase Date"),
    ("status", "Order Status")
]

# Output configuration
OUTPUT_DIR = "./output"
LOG_DIR = "./logs"

# Ensureing directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logger():
    """Configure daily rotating error logger"""
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(LOG_DIR, f"{today}.log")
    
    logger = logging.getLogger("api_etl_logger")
    logger.setLevel(logging.ERROR)

    # Avoid adding multiple handlers in case of multiple calls
    if not logger.handlers:
        handler = logging.FileHandler(log_file, mode="a")
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def get_access_token():
    """Obtain OAuth2 access token using client credentials flow"""
    token_payload = {
        "grant_type": GRANT_TYPE,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": SCOPE
    }
    
    token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    
    response = requests.post(TOKEN_URL, data=token_payload, headers=token_headers)
    response.raise_for_status()
    
    return response.json().get("access_token")


def build_request_urls(base_url, endpoint, config, dataframes):
    """
    Generate API request URLs with filtering and selection
    Handles chunking for large filter lists to avoid URL length limits
    """
    select_clause = config.get("select", [])
    extra_filter = config.get("filter", "")
    key_filter_col = config.get("key_filter")
    
    # If we need to filter based on values from another dataset
    if key_filter_col and "driving_view_col_name" in config:
        driving_col = config["driving_view_col_name"]
        id_list = (
            dataframes["df_users"][driving_col]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        
        # Chunk requests to avoid URL length constraints
        chunk_size = 10
        for i in range(0, len(id_list), chunk_size):
            chunk = id_list[i:i + chunk_size]
            id_filter = " or ".join([f"{key_filter_col} eq '{val}'" for val in chunk])
            
            filter_str = f"{id_filter} and {extra_filter}" if extra_filter else id_filter
            url = f"{base_url}{endpoint}?$filter={filter_str}"
            
            if select_clause:
                url += f"&$select={','.join(select_clause)}"
            
            yield url
    else:
        # Simple request without dependent filtering
        url = f"{base_url}{endpoint}?"
        
        if extra_filter:
            url += f"$filter={extra_filter}"
        
        if select_clause:
            separator = "&" if extra_filter else ""
            url += f"{separator}$select={','.join(select_clause)}"
        
        yield url


def fetch_data_with_pagination(url, headers, endpoint_name, logger):
    """
    Fetch all data from paginated API endpoint
    Handles token refresh on 401 errors
    """
    all_data = []
    
    while url:
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=120)
            
            # Handle token expiration
            if response.status_code == 401:
                print(f"\nToken expired. Refreshing for {endpoint_name}...")
                new_token = get_access_token()
                headers["Authorization"] = f"Bearer {new_token}"
                response = requests.get(url, headers=headers, stream=True, timeout=120)
            
            if response.status_code != 200:
                error_msg = f"Failed request for {endpoint_name}: {response.status_code}"
                logger.error(f"{error_msg} - URL: {url}")
                raise Exception(error_msg)
            
            json_data = response.json()
            all_data.extend(json_data.get("value", []))
            
            # Get next page URL for pagination
            url = json_data.get("@odata.nextLink")
            
        except Exception as e:
            logger.error(f"Error fetching {endpoint_name}: {str(e)}", exc_info=True)
            raise
    
    return all_data


def clean_join_key(df, key_column):
    """Clean and standardize join key for reliable merging"""
    if key_column not in df.columns:
        return df
    
    df[key_column] = (
        df[key_column]
        .astype(str)
        .replace(["nan", "<NA>", "None"], "")
        .fillna("")
    )
    return df


def safe_cast_column(df, column, dtype):
    """Safely cast column to specified data type with error handling"""
    if column not in df.columns:
        return df
    
    dtype_lower = dtype.lower()
    
    if dtype_lower == "string":
        df[column] = df[column].astype(str).fillna("")
    elif dtype_lower in ["int64", "int"]:
        df[column] = (
            pd.to_numeric(df[column], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
    elif dtype_lower in ["float64", "float"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    else:
        df[column] = df[column].astype(str)
    
    return df


def safe_merge(left_df, right_df, left_key, right_key):
    """Merge dataframes with null-safe join keys"""
    left_df = clean_join_key(left_df, left_key)
    right_df = clean_join_key(right_df, right_key)
    
    return left_df.merge(
        right_df,
        left_on=left_key,
        right_on=right_key,
        how="left"
    )


def apply_transformations(df, operations, dataframes):
    """Apply a series of transformation operations to build final dataset"""
    result_df = df.copy()
    
    for operation in operations:
        action = operation[0]
        
        if action == "RENAME":
            _, old_col, new_col = operation
            result_df = result_df.rename(columns={old_col: new_col})
        
        elif action == "CAST":
            _, column, dtype = operation
            result_df = safe_cast_column(result_df, column, dtype)
        
        elif action == "JOIN":
            _, table, left_key, right_key = operation
            df_to_merge = dataframes[table].copy()
            result_df = safe_merge(result_df, df_to_merge, left_key, right_key)
    
    return result_df


def format_output_columns(df, column_mappings):
    """Rename and reorder columns based on mapping configuration"""
    series_list = [df[old].rename(new) for old, new in column_mappings if old in df.columns]
    return pd.DataFrame(series_list).T


def main():
    """Main ETL pipeline execution"""
    logger = setup_logger()
    
    try:
        print("Starting API data integration pipeline...")
        
        # Get OAuth2 access token
        print("\nAuthenticating...")
        access_token = get_access_token()
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Prefer": "odata.maxpagesize=100000",
            "Connection": "close"
        }
        
        # Fetch data from all configured endpoints
        for endpoint, config in ENDPOINT_CONFIGS.items():
            print(f"\nFetching data from: {endpoint}")
            all_data = []
            
            for url in build_request_urls(BASE_URL, endpoint, config, dataframes):
                data = fetch_data_with_pagination(url, headers, endpoint, logger)
                all_data.extend(data)
            
            # Convert to DataFrame
            df = pd.DataFrame(all_data)
            expected_cols = config.get("select", [])
            df = df.reindex(columns=expected_cols)
            
            dataframes[f"df_{endpoint}"] = df
            print(f"Total rows fetched: {df.shape[0]}")
        
        # Build final dataset through transformations
        print("\nApplying transformations...")
        final_df = dataframes["df_users"].copy()
        final_df = apply_transformations(final_df, JOIN_OPERATIONS, dataframes)
        
        # Format output
        output_df = format_output_columns(final_df, COLUMN_MAPPINGS)
        
        # Apply date formatting (example)
        date_columns = ["Purchase Date"]
        for col in date_columns:
            if col in output_df.columns:
                output_df[col] = (
                    pd.to_datetime(output_df[col], utc=True, errors="coerce")
                    .dt.tz_convert("UTC")
                    .dt.strftime("%Y-%m-%d %H:%M")
                )
        
        # Apply value mappings (example)
        if "Order Status" in output_df.columns:
            output_df["Order Status"] = (
                output_df["Order Status"]
                .astype(str)
                .str.strip()
                .str.lower()
                .map({
                    "0": "Pending",
                    "1": "Completed",
                    "2": "Cancelled"
                })
            )
        
        # Export to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(OUTPUT_DIR, f"api_export_{timestamp}.csv")
        output_df.to_csv(output_file, index=False)
        
        print(f"\n Export completed: {output_file}")
        print(f" Total records: {output_df.shape[0]}")
        print(f" Columns: {output_df.shape[1]}")
        
    except Exception as e:
        print(f"\n Pipeline failed: {str(e)}")
        logger.error("Pipeline execution failed", exc_info=True)
        raise
    
    finally:
        logging.shutdown()


if __name__ == "__main__":
    main()
