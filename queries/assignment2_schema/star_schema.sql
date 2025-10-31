-- DuckDB: build star schema tables from the CSV-loaded tables, then a summary

CREATE SCHEMA IF NOT EXISTS data_warehouse;

-- Dimensions
CREATE OR REPLACE TABLE data_warehouse.DateDim AS
SELECT
  CAST(DateKey AS INTEGER)   AS DateKey,
  CAST(FullDate AS DATE)     AS FullDate,
  CAST(Day AS INTEGER)       AS Day,
  CAST(DayOfWeek AS INTEGER) AS DayOfWeek,
  CAST(Month AS INTEGER)     AS Month,
  CAST(MonthName AS VARCHAR) AS MonthName,
  CAST(Quarter AS INTEGER)   AS Quarter,
  CAST(Year AS INTEGER)      AS Year,
  CASE WHEN LOWER(COALESCE(CAST(IsHoliday AS VARCHAR),'')) IN ('1','true','t','y','yes')
       THEN TRUE ELSE FALSE END AS IsHoliday
FROM DateDim;

CREATE OR REPLACE TABLE data_warehouse.CustomerDim AS
SELECT
  CAST(CustomerKey AS INTEGER)      AS CustomerKey,
  CAST(CustomerFullName AS VARCHAR) AS CustomerFullName,
  CAST(Gender AS VARCHAR)           AS Gender,
  TRY_CAST(Age AS INTEGER)          AS Age,
  CAST(Region AS VARCHAR)           AS Region,
  CAST(City AS VARCHAR)             AS City,
  CAST(PostalCode AS VARCHAR)       AS PostalCode,
  CAST(LoyaltyTier AS VARCHAR)      AS LoyaltyTier,
  CAST(PreferredPayment AS VARCHAR) AS PreferredPayment
FROM CustomerDim;

CREATE OR REPLACE TABLE data_warehouse.ProductDim AS
SELECT
  CAST(ProductKey AS INTEGER)   AS ProductKey,
  CAST(ProductSKU AS VARCHAR)   AS ProductSKU,
  CAST(ProductName AS VARCHAR)  AS ProductName,
  CAST(Brand AS VARCHAR)        AS Brand,
  CAST(Category AS VARCHAR)     AS Category,
  CAST(SubCategory AS VARCHAR)  AS SubCategory,
  CAST(Colour AS VARCHAR)       AS Colour,
  CAST(Size AS VARCHAR)         AS Size,
  CAST(Material AS VARCHAR)     AS Material
FROM ProductDim;

CREATE OR REPLACE TABLE data_warehouse.PromotionDim AS
SELECT
  CAST(PromotionKey AS INTEGER)     AS PromotionKey,
  CAST(PromoCode AS VARCHAR)        AS PromoCode,
  CAST(CampaignName AS VARCHAR)     AS CampaignName,
  CAST(Channel AS VARCHAR)          AS Channel,
  CAST(DiscountType AS VARCHAR)     AS DiscountType,
  TRY_CAST(StartDateKey AS INTEGER) AS StartDateKey,
  TRY_CAST(EndDateKey AS INTEGER)   AS EndDateKey,
  CAST(TargetAudience AS VARCHAR)   AS TargetAudience
FROM PromotionDim;

CREATE OR REPLACE TABLE data_warehouse.WebChannelDim AS
SELECT
  CAST(WebChannelKey AS INTEGER)  AS WebChannelKey,
  CAST(ChannelType AS VARCHAR)    AS ChannelType,
  CAST(Domain AS VARCHAR)         AS Domain,
  CAST(Platform AS VARCHAR)       AS Platform,
  CAST(DeviceType AS VARCHAR)     AS DeviceType,
  CAST(RegionOfAccess AS VARCHAR) AS RegionOfAccess
FROM WebChannelDim;

-- Fact (coerce numeric types; compute SalesAmount if missing)
CREATE OR REPLACE TABLE data_warehouse.WebSalesFact AS
SELECT
  TRY_CAST(SalesKey AS BIGINT)          AS SalesKey,
  TRY_CAST(DateKey AS INTEGER)          AS DateKey,
  TRY_CAST(CustomerKey AS INTEGER)      AS CustomerKey,
  TRY_CAST(ProductKey AS INTEGER)       AS ProductKey,
  TRY_CAST(PromotionKey AS INTEGER)     AS PromotionKey,
  TRY_CAST(WebChannelKey AS INTEGER)    AS WebChannelKey,
  CAST(OrderNumber AS VARCHAR)          AS OrderNumber,
  TRY_CAST(OrderLineNumber AS INTEGER)  AS OrderLineNumber,
  TRY_CAST(Quantity AS INTEGER)         AS Quantity,
  TRY_CAST(UnitPrice AS DOUBLE)         AS UnitPrice,
  COALESCE(TRY_CAST(DiscountAmount AS DOUBLE), 0) AS DiscountAmount,
  COALESCE(TRY_CAST(CostAmount AS DOUBLE), 0)     AS CostAmount,
  COALESCE(TRY_CAST(SalesAmount AS DOUBLE),
           (TRY_CAST(Quantity AS DOUBLE)*TRY_CAST(UnitPrice AS DOUBLE))
             - COALESCE(TRY_CAST(DiscountAmount AS DOUBLE),0))        AS SalesAmount
FROM WebSalesFact;

-- Row counts (forces output)
SELECT 'DateDim' tbl, COUNT(*) cnt FROM data_warehouse.DateDim
UNION ALL SELECT 'CustomerDim', COUNT(*) FROM data_warehouse.CustomerDim
UNION ALL SELECT 'ProductDim', COUNT(*) FROM data_warehouse.ProductDim
UNION ALL SELECT 'PromotionDim', COUNT(*) FROM data_warehouse.PromotionDim
UNION ALL SELECT 'WebChannelDim', COUNT(*) FROM data_warehouse.WebChannelDim
UNION ALL SELECT 'WebSalesFact', COUNT(*) FROM data_warehouse.WebSalesFact
ORDER BY tbl;

-- Monthly summary in the DW schema
CREATE OR REPLACE TABLE data_warehouse.SalesByMonthCategory AS
SELECT
  d.Year,
  d.Month,
  p.Category,
  COUNT(*) AS RowCount,
  SUM(COALESCE(TRY_CAST(f.Quantity    AS DOUBLE), 0))                    AS Qty,
  SUM(COALESCE(TRY_CAST(f.SalesAmount AS DOUBLE), 0))                    AS Sales,
  SUM(COALESCE(TRY_CAST(f.CostAmount  AS DOUBLE), 0))                    AS Cost,
  SUM(COALESCE(TRY_CAST(f.SalesAmount AS DOUBLE), 0)
    - COALESCE(TRY_CAST(f.CostAmount  AS DOUBLE), 0))                    AS Profit
FROM data_warehouse.WebSalesFact f
JOIN data_warehouse.DateDim    d ON f.DateKey    = d.DateKey
JOIN data_warehouse.ProductDim p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Month, p.Category;

-- Preview (so your runner writes a CSV)
SELECT * FROM data_warehouse.SalesByMonthCategory
ORDER BY Year, Month, Category;
