-- Query: Join Property Listings with Agent Details
SELECT 
    f.fact_id,
    p.property_code,
    p.property_address,
    p.property_type,
    f.price,
    f.status,
    f.listing_type,
    f.listed_date,
    a.agent_id,
    a.agent_name,
    a.agent_phone,
    a.agent_email
FROM primesquare.fact_dim_table f
JOIN primesquare.property_dim_table p ON f.property_id = p.property_id
JOIN primesquare.agent_dim_table a ON f.agent_id = a.agent_id
WHERE f.status = 'Active'
ORDER BY f.price DESC;