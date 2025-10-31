SELECT 
  c.Region,
  p.Category,
  d.Year,
  SUM(f.SalesAmount) AS Revenue,
  SUM(f.CostAmount) AS Cost,
  SUM(f.SalesAmount - f.CostAmount) AS Profit
FROM WebSalesFact f
JOIN CustomerDim c ON f.CustomerKey = c.CustomerKey
JOIN ProductDim p ON f.ProductKey = p.ProductKey
JOIN DateDim d ON f.DateKey = d.DateKey
GROUP BY CUBE (c.Region, p.Category, d.Year);