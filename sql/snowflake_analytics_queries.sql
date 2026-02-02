
-- ============================================================================
-- SNOWFLAKE ANALYTICS QUERIES - ANIME ANALYTICS PLATFORM
-- ============================================================================
-- Database: ANIME_ANALYTICS_DB
-- Schemas: RAW, STAGING, ANALYTICS
-- ============================================================================
-- PROJECT 1: ANIME VIEWERSHIP ANALYSIS
-- ============================================================================

-- Q1: Which studios produce the highest-quality anime?
SELECT 
    studios,
    COUNT(*) as production_count,
    ROUND(AVG(score), 2) as avg_score,
    MAX(score) as best_score,
    MIN(score) as worst_score
FROM ANIME_ANALYTICS_DB.STAGING.anime_viewership_staging
WHERE studios IS NOT NULL AND score > 0
GROUP BY studios
ORDER BY avg_score DESC
LIMIT 15;

-- Q2: Genre performance analysis

-- ============================================================================
-- PROJECT 4: LICENSING STRATEGY ANALYSIS
-- ============================================================================

-- Q1: Exclusive vs Shared Licensing Performance
SELECT 
    is_exclusive,
    CASE WHEN is_exclusive = 1 THEN 'Exclusive' ELSE 'Shared' END as licensing_type,
    COUNT(*) as deal_count,
    ROUND(AVG(roi_percent), 2) as avg_roi,
    ROUND(AVG(licensing_cost_usd), 0) as avg_cost,
    ROUND(AVG(estimated_attributed_revenue_usd), 0) as avg_revenue
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging
GROUP BY is_exclusive
ORDER BY avg_roi DESC;

-- Q2: ROI by Region
SELECT 
    region_code,
    COUNT(*) as deal_count,
    ROUND(AVG(roi_percent), 2) as avg_roi,
    ROUND(AVG(licensing_cost_usd), 0) as avg_cost,
    ROUND(AVG(estimated_attributed_revenue_usd), 0) as avg_revenue,
    ROUND(SUM(licensing_cost_usd), 0) as total_spend
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging
GROUP BY region_code
ORDER BY avg_roi DESC;

-- Q3: Anime Quality Tier vs ROI Performance
SELECT 
    CASE 
        WHEN anime_score < 7 THEN 'Low (<7)'
        WHEN anime_score < 8 THEN 'Medium (7-8)'
        WHEN anime_score < 9 THEN 'High (8-9)'
        ELSE 'Premium (9+)'
    END as quality_tier,
    COUNT(*) as deal_count,
    ROUND(AVG(roi_percent), 2) as avg_roi,
    ROUND(AVG(licensing_cost_usd), 0) as avg_cost,
    ROUND(AVG(estimated_attributed_revenue_usd), 0) as avg_revenue
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging
GROUP BY quality_tier
ORDER BY avg_roi DESC;

-- Q4: Licensing Cost Efficiency
SELECT 
    anime_title,
    region_code,
    licensing_type,
    licensing_cost_usd,
    estimated_attributed_revenue_usd,
    roi_percent,
    payback_period_months
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging
ORDER BY roi_percent DESC
LIMIT 20;

-- Q5: Summary Statistics - Full Portfolio
SELECT 
    'Total Deals' as metric,
    CAST(COUNT(*) as VARCHAR) as value
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging

UNION ALL

SELECT 
    'Total Licensing Spend',
    '$' || CAST(ROUND(SUM(licensing_cost_usd), 0) as VARCHAR)
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging

UNION ALL

SELECT 
    'Total Estimated Revenue',
    '$' || CAST(ROUND(SUM(estimated_attributed_revenue_usd), 0) as VARCHAR)
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging

UNION ALL

SELECT 
    'Average Portfolio ROI',
    CAST(ROUND(AVG(roi_percent), 2) as VARCHAR) || '%'
FROM ANIME_ANALYTICS_DB.STAGING.licensing_roi_staging;
