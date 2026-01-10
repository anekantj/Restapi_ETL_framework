"""
config.py - Configuration file for ETL Pipeline
This file contains all endpoint definitions, transformation steps, and processing rules
"""

# ============================================================================
# API ENDPOINT CONFIGURATIONS
# Define what data to fetch from each API endpoint
# ============================================================================

VIEW_CONFIGS = {
    # Primary/Driving table - fetched first, drives subsequent dependent fetches
    "customers": {
        "select": [
            "customer_id",
            "customer_name", 
            "email",
            "phone",
            "region",
            "account_status",
            "created_date",
            "last_modified_date"
        ],
        "filter": "account_status eq 'active'",  # Only fetch active customers
        "key_filter": None,  # Independent fetch (not filtered by another table)
        "driving_view_col_name": None,
        "rename": None
    },
    
    # Dependent table - filtered based on customers fetched above
    "orders": {
        "select": [
            "order_id",
            "customer_id",
            "order_date",
            "total_amount",
            "status_code",
            "priority_flag",
            "shipping_address",
            "payment_method"
        ],
        "filter": "",  # Additional filters can be added here
        "key_filter": "customer_id",  # This field will be filtered
        "driving_view_col_name": "customer_id",  # Use these values from customers table
        "rename": None
    },
    
    # Another dependent table - filtered based on orders
    "order_items": {
        "select": [
            "item_id",
            "order_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "discount_applied"
        ],
        "filter": "",
        "key_filter": "order_id",
        "driving_view_col_name": "order_id",
        "rename": None
    },
    
    # Independent reference table - fetched separately
    "products": {
        "select": [
            "product_id",
            "product_name",
            "category",
            "brand",
            "list_price",
            "in_stock",
            "discontinued"
        ],
        "filter": "discontinued eq false",
        "key_filter": None,
        "driving_view_col_name": None,
        "rename": None
    }
}


# ============================================================================
# TRANSFORMATION PIPELINE STEPS
# Sequential operations to build the final dataset
# Format: (ACTION, parameters...)
# ============================================================================

TRANSFORMATION_STEPS = [
    # Step 1: Rename columns in the driving table for consistency
    ("RENAME", "customer_id", "cust_id"),
    ("RENAME", "customer_name", "name"),
    ("RENAME", "created_date", "customer_since"),
    
    # Step 2: Type casting for reliable joins
    ("CAST", "cust_id", "string"),
    ("CAST", "phone", "string"),
    
    # Step 3: Join orders to customers
    ("JOIN", "df_orders", "cust_id", "customer_id"),
    
    # Step 4: Type conversions after join
    ("CAST", "total_amount", "int64"),
    ("CAST", "order_id", "string"),
    ("CAST", "status_code", "int64"),
    
    # Step 5: Join order items to the combined dataset
    ("JOIN", "df_order_items", "order_id", "order_id"),
    
    # Step 6: Final type conversions
    ("CAST", "quantity", "int64"),
    ("CAST", "unit_price", "int64"),
    ("CAST", "product_id", "string"),
    
    # Step 7: Optional - Join product details
    ("JOIN", "df_products", "product_id", "product_id"),
]


# ============================================================================
# POST-PROCESSING CONFIGURATIONS
# Define how to format and standardize the final output
# ============================================================================

# DateTime columns to be formatted
DATETIME_COLUMNS = [
    "customer_since",
    "order_date",
    "last_modified_date"
]

# DateTime format settings
DATETIME_FORMAT = "%d/%m/%Y %H:%M"  # Day/Month/Year Hour:Minute
SOURCE_TIMEZONE = "UTC"
TARGET_TIMEZONE = "UTC"  # Change to your target timezone (e.g., "Asia/Kolkata", "America/New_York")

# Boolean columns to normalize (TRUE/FALSE → Yes/No)
BOOLEAN_COLUMNS = [
    "priority_flag",
    "discount_applied",
    "in_stock"
]

# Status code mappings (numeric → human-readable)
STATUS_CODE_MAPPING = {
    0: "Cancelled",
    1: "Pending",
    2: "Processing",
    3: "Shipped",
    4: "Delivered",
    5: "Returned"
}
