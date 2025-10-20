-- Query: Agents Selling Above Average Price
WITH avg_price AS (
    SELECT AVG(price) AS avg_price
    FROM primesquare.fact_dim_table
    WHERE price IS NOT NULL
)
SELECT 
    a.agent_id,
    a.agent_name,
    a.agent_phone,
    a.agent_email,
    COUNT(f.fact_id) AS properties_sold,
    AVG(f.price) AS avg_sale_price
FROM primesquare.fact_dim_table f
JOIN primesquare.agent_dim_table a ON f.agent_id = a.agent_id
WHERE f.price > (SELECT avg_price FROM avg_price)
GROUP BY a.agent_id, a.agent_name, a.agent_phone, a.agent_email
HAVING COUNT(f.fact_id) > 0
ORDER BY avg_sale_price DESC;