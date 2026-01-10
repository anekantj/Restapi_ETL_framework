# Restapi_ETL_framework

A production-grade ETL (Extract, Transform, Load) pipeline for integrating data from OAuth2-protected REST APIs with support for complex transformations, business-user-friendly configuration, and enterprise resilience features.

## Project Overview

This pipeline was designed to solve the challenge of extracting and transforming data from multiple related API endpoints where:
- Data relationships require dependent fetching (child records filtered by parent IDs)
- Business users need to control column mappings without touching code
- Large datasets require chunking to avoid API URL length constraints
- Token expiration can occur during long-running data fetches
- Legacy systems require careful null handling and type compatibility

## ✨ Key Features

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
