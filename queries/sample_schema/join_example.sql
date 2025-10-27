-- JOIN Example: Combining Fact and Dimension Tables
-- This query demonstrates how to join fact tables with dimension tables
-- to create meaningful business reports

SELECT 
    c.customer_name,
    c.city,
    c.region,
    c.segment,
    p.product_name,
    p.category,
    p.brand,
    f.sale_date,
    f.quantity,
    f.unit_price,
    f.total_amount,
    f.discount_percent,
    f.sales_rep,
    -- Calculate profit margin
    (f.total_amount - (p.cost * f.quantity)) as profit,
    -- Calculate profit margin percentage
    ROUND(((f.total_amount - (p.cost * f.quantity)) / f.total_amount) * 100, 2) as profit_margin_pct
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
WHERE f.sale_date >= '2024-01-01'
ORDER BY f.sale_date DESC, f.total_amount DESC;