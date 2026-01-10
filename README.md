# RestAPI_ETL_framework

A production-grade ETL (Extract, Transform, Load) pipeline for integrating data from OAuth2-protected REST APIs with support for complex transformations, business-user-friendly configuration, and enterprise resilience features.

## 1. Project Overview

This pipeline was designed to solve the challenge of extracting and transforming data from multiple related API endpoints where:
- Data relationships require dependent fetching (child records filtered by parent IDs)
- Large datasets require chunking to avoid API URL length constraints
- Token expiration can occur during long-running data fetches
- Legacy systems require careful null handling and type compatibility

## 2. Key Features

### Architecture & Design
- **Config-Driven**: All endpoints, filters, and transformations defined in configuration, not code
- **Excel-Based Column Mapping**: Business users can update field mappings without developer involvement
- **Multi-Step Transformation Pipeline**: Sequential RENAME → CAST → JOIN operations for complex data modeling
- **Driving Table Pattern**: Automatically fetches related data based on primary dataset values

### Resilience & Performance
- **Automatic Token Refresh**: Handles OAuth2 token expiration mid-pipeline without data loss
- **Intelligent Chunking**: Splits large filter lists into chunks of 10 to avoid URL length limits
- **Pagination Handling**: Seamlessly processes OData `@odata.nextLink` for large result sets
- **Null-Safe Operations**: Compatible with legacy pandas versions that handle NaN inconsistently

### Data Quality
- **Type-Safe Transformations**: Robust casting with fallback values for data integrity
- **Standardization Pipeline**: Consistent datetime formatting, boolean normalization, and status code mapping
- **Error Logging**: Daily rotating logs with full exception stack traces for debugging

## 3. Technical Highlights

### A. Dependent Data Fetching
```python
# Automatically filters child records based on parent dataset
if key_filter_col and driving_col:
    id_list = dfs["df_customers"][driving_col].unique()
    # Chunks into groups of 10 to avoid URL length limits
    for chunk in chunks(id_list, size=10):
        filter = " or ".join([f"customer_id eq '{id}'" for id in chunk])
        # Fetches only relevant orders for these customers
```

### B. Multi-Step Transformation Pattern
```python
TRANSFORMATION_STEPS = [
    ("RENAME", "customer_id", "cust_id"),      # Standardize names
    ("CAST", "cust_id", "string"),             # Ensure join compatibility
    ("JOIN", "df_orders", "cust_id", "customer_id"),  # Merge datasets
    ("CAST", "total_amount", "int64"),         # Type safety
]
# Executes in sequence to build final dataset
```

### C. Null-Safe Join Operations
```python
def safe_merge(left_df, right_df, left_key, right_key):
    # Handles NaN, None, <NA>, "nan" string variations
    # Critical for legacy pandas versions (pre-1.0)
    left_df[left_key] = left_df[left_key].astype(str).replace(["nan", "<NA>"], "")
    # Ensures reliable joins across systems
```

### D. Business-User Column Mapping
```excel
| old_column_name  | new_column_name    |
|------------------|--------------------|
| cust_id          | Customer ID        |
| order_date       | Purchase Date      |
| total_amount     | Order Total        |
```

## 4. Performance Metrics

- **Throughput**: Handles 100,000+ records with pagination
- **Resilience**: Auto-recovery from token expiration during multi-hour runs
- **Efficiency**: Chunking reduces API calls by 90% vs. individual ID requests
- **Maintainability**: 80% of changes require only config updates, no code changes

## 5.Technology Stack

- **Python 3.x**
- **pandas**: Data manipulation and transformation
- **requests**: HTTP client with streaming support
- **python-dotenv**: Environment variable management
- **openpyxl**: Excel file reading for business mappings

## 6. Project Structure

```
etl-pipeline/
├── main.py                      # Core ETL engine
├── config/
│   ├── column_mappings.xlsx     # Business-controlled field mappings
│   └── Column_mapping.csv       # csv mapping 
├── output/                      # Timestamped CSV exports
├── logs/                        # Daily error logs
├── .env                         # API credentials
└── README.md
```


## 7. What I Learned

### Technical Skills
- Designed config-driven architecture for maximum flexibility
- Implemented OAuth2 token lifecycle management in long-running processes
- Solved URL length constraints through intelligent request chunking
- Built null-safe data operations for legacy system compatibility

### System Design
- Created reusable transformation pipeline pattern (applicable to any ETL scenario)
- Implemented graceful degradation (logging + retry) for production resilience

### Problem Solving
- **Challenge**: API URL length limits with 1000+ filter IDs
  - **Solution**: Chunking algorithm to batch requests in groups of 10

- **Challenge**: Token expiration during 1-hour data fetches
  - **Solution**: Automatic refresh on 401 with request retry

- **Challenge**: Inconsistent null handling across pandas versions
  - **Solution**: Custom safe_merge/safe_cast utilities

## 8. Future Enhancements

- [ ] Parallel endpoint fetching for improved performance
- [ ] Data validation layer with configurable business rules
- [ ] Webhook notifications on pipeline completion/failure
- [ ] Database output support (currently set to CSV only due to legacy systems)
