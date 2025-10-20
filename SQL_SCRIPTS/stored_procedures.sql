-- Stored Procedure: Get Properties by City and Price Range
CREATE OR REPLACE PROCEDURE primesquare.get_properties_by_city_price(
    p_city VARCHAR(100),
    p_min_price DECIMAL(15,2),
    p_max_price DECIMAL(15,2)
)
LANGUAGE SQL
AS $$
    SELECT 
        p.property_id,
        p.property_code,
        p.property_address,
        p.property_type,
        p.bedrooms,
        p.bathrooms,
        p.square_footage,
        p.year_built,
        p.lot_size,
        f.price,
        f.status,
        l.city,
        l.state,
        l.zip_code
    FROM primesquare.fact_dim_table f
    JOIN primesquare.property_dim_table p ON f.property_id = p.property_id
    JOIN primesquare.location_dim_table l ON f.location_id = l.location_id
    WHERE l.city = p_city
    AND f.price BETWEEN p_min_price AND p_max_price
    ORDER BY f.price DESC;
$$;

-- Stored Procedure: Update Property Status
CREATE OR REPLACE PROCEDURE primesquare.update_property_status(
    p_fact_id INTEGER,
    p_new_status VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE primesquare.fact_dim_table
    SET status = p_new_status,
        last_seen_date = CURRENT_TIMESTAMP
    WHERE fact_id = p_fact_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Fact ID % not found', p_fact_id;
    END IF;
    
    COMMIT;
END;
$$;





