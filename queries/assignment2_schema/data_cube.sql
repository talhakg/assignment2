-- Task 3: Create a data cube with 3 measures and 3 dimensions (includes the Year)

-- Step 1: Get the base data and join the Fact and Dimensions (measure group + dimensions)
WITH base_cube AS (
  SELECT
      c.Region      AS region,       -- Dimension 1
      p.Category    AS category,     -- Dimension 2
      d.Year        AS year,         -- Dimension 3
      f.Quantity    AS quantity,     -- Measure 1
      f.SalesAmount AS sales_amount, -- Measure 2
      f.CostAmount  AS cost_amount   -- Measure 3
  
  FROM data_warehouse.WebSalesFact f
  JOIN data_warehouse.CustomerDim  c ON f.CustomerKey = c.CustomerKey
  JOIN data_warehouse.ProductDim   p ON f.ProductKey  = p.ProductKey
  JOIN data_warehouse.DateDim      d ON f.DateKey     = d.DateKey
),


-- Step 2: Add the total for each region, category, and year (3D level, measure group)
summary_cube AS (
  SELECT region, category, year,
      SUM(quantity)     AS total_quantity,
      SUM(sales_amount) AS total_sales,
      SUM(cost_amount)  AS total_cost
  
  FROM base_cube
  GROUP BY region, category, year
)


-- Step 3: Show all the cube levels from 3D - 0D to build the hierarchy
SELECT region, category, year,
  total_quantity AS Quantity,
  total_sales    AS SalesAmount,
  total_cost     AS CostAmount

FROM summary_cube


-- UNION ALL method adds the lower-level summaries (2D/1D) to get the grand total (hierarchy)
UNION ALL
SELECT region, category, NULL,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY region, category

UNION ALL
SELECT region, NULL, year,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY region, year

UNION ALL
SELECT NULL, category, year,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY category, year

UNION ALL
SELECT region, NULL, NULL,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY region

UNION ALL
SELECT NULL, category, NULL,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY category

UNION ALL
SELECT NULL, NULL, year,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube
GROUP BY year

UNION ALL
SELECT NULL, NULL, NULL,
       SUM(total_quantity), SUM(total_sales), SUM(total_cost)
FROM summary_cube