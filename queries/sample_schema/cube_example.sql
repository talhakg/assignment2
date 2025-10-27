-- CUBE Example: Multi-dimensional Analysis
-- This query demonstrates CUBE functionality for analyzing sales data
-- across multiple dimensions (region, category, age_group)

SELECT 
    c.region,
    p.category,
    c.age_group,
    COUNT(*) as total_orders,
    SUM(f.total_amount) as total_sales,
    AVG(f.total_amount) as avg_order_value,
    SUM(f.quantity) as total_quantity
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY CUBE (c.region, p.category, c.age_group)
ORDER BY 
    c.region NULLS LAST,
    p.category NULLS LAST,
    c.age_group NULLS LAST;