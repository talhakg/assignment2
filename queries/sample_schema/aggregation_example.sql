-- Aggregation Example: Sales Performance Analysis
-- This query demonstrates various aggregation functions and grouping
-- to analyze sales performance across different dimensions

SELECT 
    c.region,
    p.category,
    c.segment,
    -- Sales metrics
    COUNT(DISTINCT f.sale_id) as number_of_orders,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    SUM(f.quantity) as total_units_sold,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value,
    MIN(f.total_amount) as min_order_value,
    MAX(f.total_amount) as max_order_value,
    
    -- Discount analysis
    AVG(f.discount_percent) as avg_discount_pct,
    SUM(CASE WHEN f.discount_percent > 0 THEN 1 ELSE 0 END) as discounted_orders,
    
    -- Profit analysis (estimated)
    SUM(f.total_amount - (p.cost * f.quantity)) as estimated_profit,
    AVG(f.total_amount - (p.cost * f.quantity)) as avg_profit_per_order,
    
    -- Date analysis
    MIN(f.sale_date) as first_sale_date,
    MAX(f.sale_date) as last_sale_date
    
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY c.region, p.category, c.segment
HAVING SUM(f.total_amount) > 500  -- Only show combinations with revenue > $500
ORDER BY total_revenue DESC;