-- =============================================================================
-- Hacker News Data Lake — Athena Query Templates
-- Database: hn_data_lake | Table: stories
-- Partition columns: year, month, day
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. One-time setup: repair partitions after Glue crawl
-- -----------------------------------------------------------------------------
MSCK REPAIR TABLE hn_data_lake.stories;


-- =============================================================================
-- SECTION 1: Daily snapshots
-- =============================================================================

-- Top 10 stories by score for a specific date
SELECT
    id,
    title,
    url,
    domain,
    by         AS author,
    score,
    descendants AS total_comments,
    kids_count  AS direct_replies
FROM hn_data_lake.stories
WHERE year = '2024'
  AND month = '01'
  AND day = '15'
ORDER BY score DESC
LIMIT 10;


-- Story count and average score per day (last 30 days)
SELECT
    run_date,
    COUNT(*)          AS story_count,
    AVG(score)        AS avg_score,
    MAX(score)        AS max_score,
    SUM(descendants)  AS total_comments
FROM hn_data_lake.stories
WHERE year = '2024'
  AND month IN ('01', '12')
GROUP BY run_date
ORDER BY run_date DESC
LIMIT 30;


-- =============================================================================
-- SECTION 2: Domain / source analysis
-- =============================================================================

-- Top 20 domains by total score (last 7 days)
SELECT
    domain,
    COUNT(*)       AS story_count,
    SUM(score)     AS total_score,
    AVG(score)     AS avg_score,
    MAX(score)     AS best_score
FROM hn_data_lake.stories
WHERE domain != ''
  AND run_date >= DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
GROUP BY domain
ORDER BY total_score DESC
LIMIT 20;


-- Stories with no URL (Ask HN / Show HN self-posts)
SELECT
    id,
    title,
    by     AS author,
    score,
    descendants AS comments,
    run_date
FROM hn_data_lake.stories
WHERE (url IS NULL OR url = '')
  AND score > 100
ORDER BY score DESC
LIMIT 50;


-- =============================================================================
-- SECTION 3: Author analysis
-- =============================================================================

-- Most prolific authors in top stories (score > 50)
SELECT
    by          AS author,
    COUNT(*)    AS stories_in_top,
    AVG(score)  AS avg_score,
    MAX(score)  AS best_score,
    SUM(descendants) AS total_comments_generated
FROM hn_data_lake.stories
WHERE score > 50
  AND by != ''
GROUP BY by
ORDER BY stories_in_top DESC
LIMIT 25;


-- Author consistency: appeared on multiple days
SELECT
    by              AS author,
    COUNT(DISTINCT run_date) AS days_active,
    COUNT(*)                 AS total_stories,
    AVG(score)               AS avg_score
FROM hn_data_lake.stories
WHERE by != ''
GROUP BY by
HAVING COUNT(DISTINCT run_date) > 3
ORDER BY days_active DESC
LIMIT 20;


-- =============================================================================
-- SECTION 4: Engagement analysis
-- =============================================================================

-- High score but low comment ratio (viral but not controversial)
SELECT
    id,
    title,
    domain,
    score,
    descendants                                        AS comments,
    ROUND(CAST(score AS DOUBLE) / NULLIF(descendants, 0), 2) AS score_per_comment,
    run_date
FROM hn_data_lake.stories
WHERE score > 200
  AND descendants > 0
ORDER BY score_per_comment DESC
LIMIT 20;


-- Stories with most direct replies (kids_count) vs total comment tree
SELECT
    id,
    title,
    score,
    kids_count   AS direct_replies,
    descendants  AS total_tree,
    descendants - kids_count AS nested_comments,
    run_date
FROM hn_data_lake.stories
WHERE descendants > 50
ORDER BY kids_count DESC
LIMIT 20;


-- Score distribution buckets
SELECT
    CASE
        WHEN score < 50   THEN '0-49'
        WHEN score < 100  THEN '50-99'
        WHEN score < 200  THEN '100-199'
        WHEN score < 500  THEN '200-499'
        WHEN score < 1000 THEN '500-999'
        ELSE '1000+'
    END AS score_bucket,
    COUNT(*) AS story_count,
    AVG(descendants) AS avg_comments
FROM hn_data_lake.stories
GROUP BY 1
ORDER BY MIN(score);


-- =============================================================================
-- SECTION 5: Trend detection
-- =============================================================================

-- Domain velocity: domains gaining traction week over week
WITH this_week AS (
    SELECT domain, COUNT(*) AS stories_this_week, AVG(score) AS avg_score_this_week
    FROM hn_data_lake.stories
    WHERE run_date >= DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
      AND domain != ''
    GROUP BY domain
),
last_week AS (
    SELECT domain, COUNT(*) AS stories_last_week, AVG(score) AS avg_score_last_week
    FROM hn_data_lake.stories
    WHERE run_date BETWEEN
        DATE_FORMAT(DATE_ADD('day', -14, CURRENT_DATE), '%Y-%m-%d')
        AND DATE_FORMAT(DATE_ADD('day', -8, CURRENT_DATE), '%Y-%m-%d')
      AND domain != ''
    GROUP BY domain
)
SELECT
    t.domain,
    t.stories_this_week,
    COALESCE(l.stories_last_week, 0)  AS stories_last_week,
    t.stories_this_week - COALESCE(l.stories_last_week, 0) AS week_delta,
    ROUND(t.avg_score_this_week, 1)   AS avg_score_this_week
FROM this_week t
LEFT JOIN last_week l USING (domain)
WHERE t.stories_this_week >= 3
ORDER BY week_delta DESC
LIMIT 20;


-- Daily top story (highest score per day)
SELECT
    run_date,
    MAX_BY(title,  score) AS top_story,
    MAX_BY(domain, score) AS top_domain,
    MAX_BY(by,     score) AS top_author,
    MAX(score)            AS top_score
FROM hn_data_lake.stories
GROUP BY run_date
ORDER BY run_date DESC
LIMIT 30;


-- =============================================================================
-- SECTION 6: Data quality checks
-- =============================================================================

-- Null / empty field audit per partition
SELECT
    run_date,
    COUNT(*)                                         AS total,
    SUM(CASE WHEN url = '' OR url IS NULL THEN 1 ELSE 0 END)    AS missing_url,
    SUM(CASE WHEN by = '' OR by IS NULL THEN 1 ELSE 0 END)      AS missing_author,
    SUM(CASE WHEN title = '' OR title IS NULL THEN 1 ELSE 0 END) AS missing_title,
    SUM(CASE WHEN score = 0 THEN 1 ELSE 0 END)      AS zero_score
FROM hn_data_lake.stories
GROUP BY run_date
ORDER BY run_date DESC;


-- Duplicate detection (same story_id across multiple run dates)
SELECT
    id,
    COUNT(DISTINCT run_date) AS run_dates,
    MIN(run_date)            AS first_seen,
    MAX(run_date)            AS last_seen,
    MAX(score)               AS max_score
FROM hn_data_lake.stories
GROUP BY id
HAVING COUNT(DISTINCT run_date) > 1
ORDER BY run_dates DESC
LIMIT 20;
