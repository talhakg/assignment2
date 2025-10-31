-- DuckDB: build DW tables from the CSV-loaded staging tables, plus a summary and a CUBE
-- Assumes the runner already loaded these staging tables from CSVs:
--   CustomerDim, DateDim, ProductDim, PromotionDim, WebChannelDim, WebSalesFact

------------------------------------------------------------
-- 1) Create a schema to hold the warehouse tables
------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS data_warehouse;

------------------------------------------------------------
-- 2) Dimensions (coerce types from CSV text to proper types)
------------------------------------------------------------
CREATE OR REPLACE TABLE data_warehouse.DateDim AS
SELECT
  CAST(DateKey AS INTEGER)                 AS DateKey,
  CAST(FullDate AS DATE)                   AS FullDate,
  CAST(Day AS INTEGER)                     AS Day,
  CAST(DayOfWeek AS INTEGER)               AS DayOfWeek,
  CAST(Month AS INTEGER)                   AS Month,
  CAST(MonthName AS VARCHAR)               AS MonthName,
  CAST(Quarter AS INTEGER)                 AS Quarter,
  CAST(Year AS INTEGER)                    AS Year,
  CASE
    WHEN LOWER(COALESCE(CAST(IsHoliday AS VARCHAR), '')) IN ('1','true','t','y','yes')
      THEN TRUE ELSE FALSE
  END                                      AS IsHoliday
FROM DateDim;

CREATE OR REPLACE TABLE data_warehouse.CustomerDim AS
SELECT
  CAST(CustomerKey AS INTEGER)             AS CustomerKey,
  CAST(CustomerFullName AS VARCHAR)        AS CustomerFullName,
  CAST(Gender AS VARCHAR)                  AS Gender,
  TRY_CAST(Age AS INTEGER)                 AS Age,
  CAST(Region AS VARCHAR)                  AS Region,
  CAST(City AS VARCHAR)                    AS City,
  CAST(PostalCode AS VARCHAR)              AS PostalCode,
  CAST(LoyaltyTier AS VARCHAR)             AS LoyaltyTier,
  CAST(PreferredPayment AS VARCHAR)        AS PreferredPayment
FROM CustomerDim;

CREATE OR REPLACE TABLE data_warehouse.ProductDim AS
SELECT
  CAST(ProductKey AS INTEGER)              AS ProductKey,
  CAST(ProductSKU AS VARCHAR)              AS ProductSKU,
  CAST(ProductName AS VARCHAR)             AS ProductName,
  CAST(Brand AS VARCHAR)                   AS Brand,
  CAST(Category AS VARCHAR)                AS Category,
  CAST(SubCategory AS VARCHAR)             AS SubCategory,
  CAST(Colour AS VARCHAR)                  AS Colour,
  CAST(Size AS VARCHAR)                    AS Size,
  CAST(Material AS VARCHAR)                AS Material
FROM ProductDim;

CREATE OR REPLACE TABLE data_warehouse.PromotionDim AS
SELECT
  CAST(PromotionKey AS INTEGER)            AS PromotionKey,
  CAST(PromoCode AS VARCHAR)               AS PromoCode,
  CAST(CampaignName AS VARCHAR)            AS CampaignName,
  CAST(Channel AS VARCHAR)                 AS Channel,
  CAST(DiscountType AS VARCHAR)            AS DiscountType,
  TRY_CAST(StartDateKey AS INTEGER)        AS StartDateKey,
  TRY_CAST(EndDateKey AS INTEGER)          AS EndDateKey,
  CAST(TargetAudience AS VARCHAR)          AS TargetAudience
FROM PromotionDim;

CREATE OR REPLACE TABLE data_warehouse.WebChannelDim AS
SELECT
  CAST(WebChannelKey AS INTEGER)           AS WebChannelKey,
  CAST(ChannelType AS VARCHAR)             AS ChannelType,
  CAST(Domain AS VARCHAR)                  AS Domain,
  CAST(Platform AS VARCHAR)                AS Platform,
  CAST(DeviceType AS VARCHAR)              AS DeviceType,
  CAST(RegionOfAccess AS VARCHAR)          AS RegionOfAccess
FROM WebChannelDim;

------------------------------------------------------------
-- 3) Fact (coerce numerics; compute SalesAmount if missing)
------------------------------------------------------------
CREATE OR REPLACE TABLE data_warehouse.WebSalesFact AS
SELECT
  TRY_CAST(SalesKey AS BIGINT)             AS SalesKey,
  TRY_CAST(DateKey AS INTEGER)             AS DateKey,
  TRY_CAST(CustomerKey AS INTEGER)         AS CustomerKey,
  TRY_CAST(ProductKey AS INTEGER)          AS ProductKey,
  TRY_CAST(PromotionKey AS INTEGER)        AS PromotionKey,
  TRY_CAST(WebChannelKey AS INTEGER)       AS WebChannelKey,
  CAST(OrderNumber AS VARCHAR)             AS OrderNumber,
  TRY_CAST(OrderLineNumber AS INTEGER)     AS OrderLineNumber,
  TRY_CAST(Quantity AS INTEGER)            AS Quantity,
  TRY_CAST(UnitPrice AS DOUBLE)            AS UnitPrice,
  COALESCE(TRY_CAST(DiscountAmount AS DOUBLE), 0) AS DiscountAmount,
  COALESCE(TRY_CAST(CostAmount  AS DOUBLE), 0)    AS CostAmount,
  COALESCE(
      TRY_CAST(SalesAmount AS DOUBLE),
      (TRY_CAST(Quantity AS DOUBLE) * TRY_CAST(UnitPrice AS DOUBLE))
        - COALESCE(TRY_CAST(DiscountAmount AS DOUBLE), 0)
  )                                         AS SalesAmount
FROM WebSalesFact;

------------------------------------------------------------
-- 4) Quick counts (forces a small result set early in the log)
------------------------------------------------------------
SELECT 'CustomerDim'   AS table_name, COUNT(*) AS row_count FROM data_warehouse.CustomerDim
UNION ALL SELECT 'DateDim',        COUNT(*) FROM data_warehouse.DateDim
UNION ALL SELECT 'ProductDim',     COUNT(*) FROM data_warehouse.ProductDim
UNION ALL SELECT 'PromotionDim',   COUNT(*) FROM data_warehouse.PromotionDim
UNION ALL SELECT 'WebChannelDim',  COUNT(*) FROM data_warehouse.WebChannelDim
UNION ALL SELECT 'WebSalesFact',   COUNT(*) FROM data_warehouse.WebSalesFact
ORDER BY table_name;

------------------------------------------------------------
-- 5) Star-schema summary (LEFT JOIN so you always get rows)
------------------------------------------------------------
CREATE OR REPLACE TABLE data_warehouse.SalesByMonthCategory AS
SELECT
  COALESCE(d.Year,  -1)                        AS Year,
  COALESCE(d.Month, -1)                        AS Month,
  COALESCE(p.Category, '(Unknown)')            AS Category,
  COUNT(*)                                     AS RowCount,
  SUM(COALESCE(TRY_CAST(f.Quantity    AS DOUBLE), 0)) AS Qty,
  SUM(COALESCE(TRY_CAST(f.SalesAmount AS DOUBLE), 0)) AS Sales,
  SUM(COALESCE(TRY_CAST(f.CostAmount  AS DOUBLE), 0)) AS Cost,
  SUM(COALESCE(TRY_CAST(f.SalesAmount AS DOUBLE), 0)
    - COALESCE(TRY_CAST(f.CostAmount  AS DOUBLE), 0)) AS Profit
FROM data_warehouse.WebSalesFact f
LEFT JOIN data_warehouse.DateDim    d ON f.DateKey    = d.DateKey
LEFT JOIN data_warehouse.ProductDim p ON f.ProductKey = p.ProductKey
GROUP BY 1,2,3;

-- Preview (so the runner writes a CSV)
SELECT * FROM data_warehouse.SalesByMonthCategory
ORDER BY Year, Month, Category;

------------------------------------------------------------
-- 6) 3×3 CUBE (Region × Category × Year) with totals
--    Works even if some dimension keys are missing in the CSVs
------------------------------------------------------------
SELECT
  COALESCE(TRIM(c.Region),   'ALL Regions')                                       AS Region,
  COALESCE(TRIM(p.Category), 'ALL Categories')                                    AS Category,
  COALESCE(CAST(TRY_CAST(TRIM(d.Year) AS INTEGER) AS VARCHAR), 'ALL Years')       AS Year,
  SUM(TRY_CAST(TRIM(f.SalesAmount) AS DOUBLE))                                    AS Revenue,
  SUM(TRY_CAST(TRIM(f.CostAmount)  AS DOUBLE))                                    AS Cost,
  SUM(TRY_CAST(TRIM(f.SalesAmount) AS DOUBLE) - TRY_CAST(TRIM(f.CostAmount) AS DOUBLE)) AS Profit
FROM WebSalesFact f
LEFT JOIN CustomerDim c ON TRY_CAST(TRIM(f.CustomerKey) AS INTEGER) = TRY_CAST(TRIM(c.CustomerKey) AS INTEGER)
LEFT JOIN ProductDim  p ON TRY_CAST(TRIM(f.ProductKey)  AS INTEGER) = TRY_CAST(TRIM(p.ProductKey)  AS INTEGER)
LEFT JOIN DateDim     d ON TRY_CAST(TRIM(f.DateKey)     AS INTEGER) = TRY_CAST(TRIM(d.DateKey)     AS INTEGER)
GROUP BY CUBE (c.Region, p.Category, TRY_CAST(TRIM(d.Year) AS INTEGER))
ORDER BY
  (c.Region IS NULL),   Region,
  (p.Category IS NULL), Category,
  (TRY_CAST(TRIM(d.Year) AS INTEGER) IS NULL), Year;

