# Snowflake Setup & Integration

## Overview

This document describes the Snowflake data warehouse setup for the Anime Analytics Platform.

## Architecture

### Database Structure

```
ANIME_ANALYTICS_DB/
├── RAW (Bronze Layer)
│   ├── anime_viewership_staging
│   ├── licensing_deals_staging
│   ├── regional_performance_staging
│   └── licensing_roi_staging
│
├── STAGING (Silver Layer)
│   └── (cleaned and transformed tables)
│
└── ANALYTICS (Gold Layer - Star Schema)
    ├── Dimensions
    │   ├── dim_anime
    │   ├── dim_studios
    │   ├── dim_regions
    │   ├── dim_genres
    │   └── dim_licensing_types
    │
    └── Facts
        ├── fact_anime_ratings
        ├── fact_licensing_deals
        └── fact_licensing_roi
```

## Data Models

### Project 1: Anime Viewership

**Fact Table: fact_anime_ratings**
- Accumulating snapshot of anime ratings over time
- Key metrics: score, rank, members, favorites
- Updated as new ratings are collected

**Dimensions:**
- dim_anime: Core anime attributes
- dim_studios: Studio information
- dim_genres: Genre catalog

### Project 4: Licensing Strategy

**Fact Table: fact_licensing_roi**
- One row per anime-region licensing deal
- Key metrics: cost, revenue, ROI percentage
- Used for strategic decision-making

**Dimensions:**
- dim_anime: Anime details
- dim_regions: Regional market data
- dim_licensing_types: Exclusive vs Shared

## Key Queries

### Project 1: Studio Analysis
```sql
SELECT studios, COUNT(*) as productions, AVG(score) as avg_score
FROM STAGING.anime_viewership_staging
GROUP BY studios
ORDER BY avg_score DESC;
```

### Project 4: ROI by License Type
```sql
SELECT 
  CASE WHEN is_exclusive = 1 THEN 'Exclusive' ELSE 'Shared' END,
  AVG(roi_percent) as avg_roi
FROM STAGING.licensing_roi_staging
GROUP BY is_exclusive;
```

## Business Insights

### From Project 1
- Top studios: OLM, Bones, Madhouse
- Best performing genres: Action, Adventure, Drama
- Seasonal variation in quality and release timing

### From Project 4
- **Exclusive licensing ROI: 28.45%**
- **Shared licensing ROI: 19.23%**
- Regional strategies differ significantly
- High-quality anime (8+) justify premium licensing

## Loading New Data

To load new data:

1. Place CSV in data/ folder
2. Run ETL script:
   ```bash
   python3 scripts/etl_pipeline_project1.py
   # or
   python3 scripts/etl_pipeline_project4.py
   ```

3. In Snowflake:
   ```sql
   COPY INTO STAGING.table_name
   FROM @~/filename.csv
   FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
   ON_ERROR = CONTINUE;
   ```

## Future Enhancements

- [ ] Add automated daily loads via Snowflake Tasks
- [ ] Create Data Quality monitoring dashboard
- [ ] Implement row-level security for regional data
- [ ] Add data masking for sensitive metrics
- [ ] Create Snowflake API for real-time queries

## Questions & Support

For questions about the Snowflake setup, refer to:
- [Snowflake Documentation](https://docs.snowflake.com)
- Query examples in sql/snowflake_analytics_queries.sql