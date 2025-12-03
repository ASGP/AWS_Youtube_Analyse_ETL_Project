--1 Avg data by country
SELECT 
    country,
    AVG(views) AS avg_views,
    AVG(likes) AS avg_likes,
    AVG(comment_count) AS avg_comments
FROM processed
GROUP BY country
ORDER BY avg_views DESC;

--2 Top Like/views ratio videos
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

--3 Most Trending catergories
SELECT category_name, SUM(views) AS total_views
FROM processed
GROUP BY category_name
ORDER BY total_views DESC;

--4 catergory wise engagements
SELECT 
    category_id,
    category_name,
    CAST(AVG(views) AS DECIMAL(10,2)) AS avg_views,
    CAST(AVG(likes) AS DECIMAL(10,2)) AS avg_likes,
    CAST(AVG(comment_count) AS DECIMAL(10,2)) AS avg_comments
FROM processed
GROUP BY category_id, category_name
ORDER BY avg_views DESC
LIMIT 10;

--5 channels with highest views
SELECT 
    channel_title,
    COUNT(*) as trending_videos,
    SUM(views) as total_views,
    ROUND(AVG(views), 0) as avg_views_per_video,
    SUM(likes) as total_likes
FROM processed
GROUP BY channel_title
ORDER BY total_views DESC
LIMIT 10;

--6 category type by country
SELECT 
    country,
    category_id,
    category_name,
    COUNT(*) AS total_trending_videos
FROM processed
GROUP BY country, category_id, category_name
ORDER BY total_trending_videos DESC;

--7 channel and category respect to highest engagement with channel as comments
SELECT 
    video_id,
    channel_title,
    category_name,
    views,
    likes,
    comment_count,
    ROUND(CAST(likes AS DOUBLE) / CAST(views AS DOUBLE) * 100, 2) as like_rate,
    ROUND(CAST(comment_count AS DOUBLE) / CAST(views AS DOUBLE) * 100, 2) as comment_rate
FROM processed
WHERE views > 10000
ORDER BY comment_rate DESC
LIMIT 10;

--8 New trending videos 
SELECT 
    channel_title AS channel,
    COUNT(*) AS total_trending_count,
    CAST(AVG(views) AS BIGINT) AS avg_views,
    CAST(MAX(views) AS BIGINT) AS peak_views
FROM processed
GROUP BY channel_title
HAVING COUNT(*) >= 2 AND COUNT(*) <= 5
ORDER BY avg_views DESC
LIMIT 10;

--9 Low view but high engagement(like)
SELECT 
    title,
    channel_title,
    country,
    views,
    likes,
    comment_count,
    ROUND(CAST(likes AS DOUBLE) / CAST(views AS DOUBLE) * 100, 2) as like_rate
FROM processed
WHERE views BETWEEN 10000 AND 100000  
    AND CAST(likes AS DOUBLE) / CAST(views AS DOUBLE) > 0.05  
ORDER BY like_rate DESC
LIMIT 10;
