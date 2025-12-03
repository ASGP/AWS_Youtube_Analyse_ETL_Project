--1 Top Like/views ratio videos
SELECT 
    video_id,
    title,
    channel_title,
    views,
    likes,
    (likes * 1.0 / NULLIF(views, 0)) AS like_rate,
    country
FROM processed
WHERE views > 100000  
ORDER BY like_rate DESC
LIMIT 20;

--2 Most Trending catergories
SELECT category_name, SUM(views) AS total_views
FROM processed
GROUP BY category_name
ORDER BY total_views DESC;

