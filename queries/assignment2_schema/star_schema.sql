-- Create Schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'data_warehouse') 
EXEC('CREATE SCHEMA data_warehouse');

-- Date Dimension
DROP TABLE IF EXISTS data_warehouse.DateDim;
CREATE TABLE data_warehouse.DateDim (
  DateKey   INT         NOT NULL PRIMARY KEY,
  FullDate  DATE        NOT NULL,
  Day       INT         NOT NULL,
  DayOfWeek INT         NOT NULL,
  Month     INT         NOT NULL,
  MonthName VARCHAR(20) NOT NULL,
  Quarter   INT         NOT NULL,
  Year      INT         NOT NULL,
  IsHoliday BIT         NOT NULL DEFAULT 0
);
-- Saves the result into the outputs directory, at the end of each table
SELECT * FROM data_warehouse.DateDim;

-- Customer Dimension
DROP TABLE IF EXISTS data_warehouse.CustomerDim;
CREATE TABLE data_warehouse.CustomerDim (
  CustomerKey      INT IDENTITY PRIMARY KEY,
  CustomerFullName VARCHAR(50) NOT NULL,
  Gender           VARCHAR(20),
  Age              INT,
  Region           VARCHAR(20),
  City             VARCHAR(20),
  PostalCode       VARCHAR(10),
  LoyaltyTier      VARCHAR(20),
  PreferredPayment VARCHAR(20)
);
SELECT * FROM data_warehouse.CustomerDim;

-- Product Dimension
DROP TABLE IF EXISTS data_warehouse.ProductDim;
CREATE TABLE data_warehouse.ProductDim (
  ProductKey  INT IDENTITY PRIMARY KEY,
  ProductSKU  VARCHAR(50) NOT NULL UNIQUE,
  ProductName VARCHAR(50),
  Brand       VARCHAR(20),
  Category    VARCHAR(30),
  SubCategory VARCHAR(30),
  Colour      VARCHAR(20),
  Size        VARCHAR(20),
  Material    VARCHAR(20)
);
SELECT * FROM data_warehouse.ProductDim;

-- Promotion Dimension
DROP TABLE IF EXISTS data_warehouse.PromotionDim;
CREATE TABLE data_warehouse.PromotionDim (
  PromotionKey   INT IDENTITY PRIMARY KEY,
  PromoCode      VARCHAR(20),
  CampaignName   VARCHAR(20),
  Channel        VARCHAR(20),
  DiscountType   VARCHAR(20),
  StartDateKey   INT NULL,
  EndDateKey     INT NULL,
  TargetAudience VARCHAR(20),
-- Foreign Key relationships to Date Dimension
  CONSTRAINT FK_Promo_Start FOREIGN KEY(StartDateKey) REFERENCES data_warehouse.DateDim(DateKey),
  CONSTRAINT FK_Promo_End   FOREIGN KEY(EndDateKey)   REFERENCES data_warehouse.DateDim(DateKey)
);
SELECT * FROM data_warehouse.PromotionDim;

-- Web Channel Dimension
DROP TABLE IF EXISTS data_warehouse.WebChannelDim;
CREATE TABLE data_warehouse.WebChannelDim (
  WebChannelKey  INT IDENTITY PRIMARY KEY,
  ChannelType    VARCHAR(20),
  Domain         VARCHAR(100),
  Platform       VARCHAR(20),
  DeviceType     VARCHAR(20),
  RegionOfAccess VARCHAR(20)
);
SELECT * FROM data_warehouse.WebChannelDim;

-- Web Sales Fact
DROP TABLE IF EXISTS data_warehouse.WebSalesFact;
CREATE TABLE data_warehouse.WebSalesFact (
  SalesKey        BIGINT IDENTITY PRIMARY KEY,
  DateKey         INT           NOT NULL,
  CustomerKey     INT           NOT NULL,
  ProductKey      INT           NOT NULL,
  PromotionKey    INT           NULL,
  WebChannelKey   INT           NOT NULL,
  OrderNumber     VARCHAR(20)   NOT NULL,
  OrderLineNumber INT           NOT NULL,
  Quantity        INT           NOT NULL CHECK (Quantity > 0),
  UnitPrice       DECIMAL(10,2) NOT NULL CHECK (UnitPrice >= 0),
  DiscountAmount  DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (DiscountAmount >= 0),
  CostAmount      DECIMAL(10,2) NOT NULL DEFAULT 0 CHECK (CostAmount >= 0),
  SalesAmount     DECIMAL(10,2) NULL CHECK (SalesAmount IS NULL OR SalesAmount >= 0),
-- Foreign Key relationships
  CONSTRAINT UQ_Order           UNIQUE (OrderNumber, OrderLineNumber),
  CONSTRAINT FK_Fact_Date       FOREIGN KEY(DateKey)       REFERENCES data_warehouse.DateDim(DateKey),
  CONSTRAINT FK_Fact_Customer   FOREIGN KEY(CustomerKey)   REFERENCES data_warehouse.CustomerDim(CustomerKey),
  CONSTRAINT FK_Fact_Product    FOREIGN KEY(ProductKey)    REFERENCES data_warehouse.ProductDim(ProductKey),
  CONSTRAINT FK_Fact_Promotion  FOREIGN KEY(PromotionKey)  REFERENCES data_warehouse.PromotionDim(PromotionKey),
  CONSTRAINT FK_Fact_WebChannel FOREIGN KEY(WebChannelKey) REFERENCES data_warehouse.WebChannelDim(WebChannelKey)
);
SELECT * FROM data_warehouse.WebSalesFact;

-- Indexes
CREATE INDEX IX_Fact_DateKey       ON data_warehouse.WebSalesFact(DateKey);
CREATE INDEX IX_Fact_CustomerKey   ON data_warehouse.WebSalesFact(CustomerKey);
CREATE INDEX IX_Fact_ProductKey    ON data_warehouse.WebSalesFact(ProductKey);
CREATE INDEX IX_Fact_PromotionKey  ON data_warehouse.WebSalesFact(PromotionKey);
CREATE INDEX IX_Fact_WebChannelKey ON data_warehouse.WebSalesFact(WebChannelKey);

-- Materialized View (Summary Table)
DROP TABLE IF EXISTS data_warehouse.SalesByMonthCategory;
SELECT
  d.Year,
  d.Month,
  p.Category,
  COUNT(*)                      AS RowCount,
  SUM(f.Quantity)               AS Qty,
  SUM(ISNULL(f.SalesAmount, 0)) AS Sales,
  SUM(ISNULL(f.CostAmount, 0))  AS Cost,
  SUM(ISNULL(f.SalesAmount, 0) - ISNULL(f.CostAmount, 0)) AS Profit
INTO data_warehouse.SalesByMonthCategory
FROM data_warehouse.WebSalesFact f
JOIN data_warehouse.DateDim    d ON f.DateKey = d.DateKey
JOIN data_warehouse.ProductDim p ON f.ProductKey = p.ProductKey
GROUP BY d.Year, d.Month, p.Category;
CREATE INDEX IX_SalesByMonthCategory_YMC
ON data_warehouse.SalesByMonthCategory(Year, Month, Category);
SELECT * FROM data_warehouse.SalesByMonthCategory;