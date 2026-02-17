-- 1️⃣ View entire dataset
SELECT *
FROM gold;


-- 2️⃣ Get weather for a specific date
SELECT city, avg_temperature, max_temperature
FROM gold
WHERE date = '2026-02-16';


-- 3️⃣ Find hottest city for a given day
SELECT city, max_temperature
FROM gold
WHERE date = '2026-02-16'
ORDER BY max_temperature DESC
LIMIT 1;


-- 4️⃣ Find coldest city for a given day
SELECT city, avg_temperature
FROM gold
WHERE date = '2026-02-16'
ORDER BY avg_temperature ASC
LIMIT 1;


-- 5️⃣ Rank cities by average temperature
SELECT city, avg_temperature
FROM gold
WHERE date = '2026-02-16'
ORDER BY avg_temperature DESC;


-- 6️⃣ Rank cities by windspeed
SELECT city, avg_windspeed
FROM gold
WHERE date = '2026-02-16'
ORDER BY avg_windspeed DESC;


-- 7️⃣ Find cities with rainfall
SELECT city, total_precipitation
FROM gold
WHERE total_precipitation > 0;


-- 8️⃣ Compute overall average temperature across cities
SELECT AVG(avg_temperature) AS overall_avg_temp
FROM gold
WHERE date = '2026-02-16';


-- 9️⃣ Compute overall max temperature recorded
SELECT MAX(max_temperature) AS highest_temp
FROM gold;


-- 🔟 Daily temperature trend for a city
SELECT date, avg_temperature
FROM gold
WHERE city = 'Delhi'
ORDER BY date;


-- 1️⃣1️⃣ Daily windspeed trend for a city
SELECT date, avg_windspeed
FROM gold
WHERE city = 'Bangalore'
ORDER BY date;


-- 1️⃣2️⃣ Compare cities side-by-side for a date
SELECT city, avg_temperature, avg_windspeed
FROM gold
WHERE date = '2026-02-16';


-- 1️⃣3️⃣ Identify cities exceeding temperature threshold
SELECT city, max_temperature
FROM gold
WHERE max_temperature > 30;


-- 1️⃣4️⃣ Compute average metrics per city across all days
SELECT 
    city,
    AVG(avg_temperature) AS avg_temp,
    AVG(avg_windspeed) AS avg_wind
FROM gold
GROUP BY city;


-- 1️⃣5️⃣ Find day with highest average temperature
SELECT date, AVG(avg_temperature) AS daily_avg_temp
FROM gold
GROUP BY date
ORDER BY daily_avg_temp DESC
LIMIT 1;