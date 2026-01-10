# Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration Layer                      │
│  • VIEW_CONFIGS: Endpoint definitions & filters             │
│  • TRANSFORMATION_STEPS: Multi-step pipeline logic          │
│  • column_mappings.xlsx: Business-controlled mappings       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                     Extraction Engine                       │
│  1. OAuth2 Authentication (client_credentials flow)         │
│  2. Dynamic URL Building (dependent vs independent)         │
│  3. Chunked Requests (10 IDs per URL)                       │
│  4. Pagination Handler (process all pages)                  │
│  5. Token Refresh (on 401 errors)                           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  Transformation Pipeline                    │
│  1. Load driving table (e.g., customers)                    │
│  2. Execute transformation steps sequentially:              │
│     • RENAME: Standardize column names                      │
│     • CAST: Ensure type compatibility                       │
│     • JOIN: Merge related datasets (null-safe)              │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                 Post-Processing & Export                    │
│  1. Apply Excel column mappings                             │
│  2. Standardize datetime formats (UTC → local)              │
│  3. Map status codes (numeric → human-readable)             │
│  4. Normalize booleans (TRUE/FALSE → Yes/No)                │
│  5. Export timestamped CSV                                  │
└─────────────────────────────────────────────────────────────┘
```
##Detailed Modelling

![ETL Pipeline Architecture](Docs/architecture-diagram.md)
