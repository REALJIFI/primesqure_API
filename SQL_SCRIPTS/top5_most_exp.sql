-- Query: Top 5 Most Expensive Properties Per City
SELECT 
    t.property_id,
    t.property_code,
    t.property_address,
    t.city,
    t.price,
    t.rank_in_city
FROM (
    SELECT 
        p.property_id,
        p.property_code,
        p.property_address,
        l.city,
        f.price,
        ROW_NUMBER() OVER (PARTITION BY l.city ORDER BY f.price DESC) AS rank_in_city
    FROM primesquare.fact_dim_table f
    JOIN primesquare.property_dim_table p ON f.property_id = p.property_id
    JOIN primesquare.location_dim_table l ON f.location_id = l.location_id
    WHERE f.price IS NOT NULL
) t
WHERE t.rank_in_city <= 5
ORDER BY t.city, t.price DESC;