DROP TABLE IF EXISTS analytics.scraped_quotes.tag_engagement;

CREATE TABLE analytics.scraped_quotes.tag_engagement AS (
    WITH pageviews AS (
        SELECT
            quote,
            pageviews
        FROM "raw".scraped_quotes.pageviews
    ),
    unnested_tags AS (
        SELECT q.author, q.quote, t AS tag_value
        FROM "raw".scraped_quotes.quotes q, q.tags AS t
    )
    SELECT
        t.tag_value,
        COUNT(*)                      AS quote_count,
        COUNT(DISTINCT t.author)      AS distinct_authors,
        SUM(COALESCE(p.pageviews, 0)) AS total_pageviews,
        CAST(total_pageviews AS FLOAT) / NULLIF(quote_count, 0) AS pageview_weighted_score
    FROM unnested_tags t
    LEFT JOIN pageviews p
        ON p.quote = t.quote
    GROUP BY t.tag_value
)
