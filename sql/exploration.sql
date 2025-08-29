-- Exploración y validación inicial de la fuente SpaceParts

-- 1. Conteo total y nulos en Customers
SELECT COUNT(*) AS total_customers,
       SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END) AS null_customerid
FROM dim_Customers;

-- 2. Top registros Products
SELECT TOP 10 * FROM dim_Products;

-- 3. Validación de fact_Invoices
SELECT COUNT(*) AS total_invoices,
       MIN(invoice_date) AS min_date,
       MAX(invoice_date) AS max_date
FROM fact_Invoices;

-- 4. Validación de negativos (ejemplo que encontramos en Net_Invoice_Value)
SELECT COUNT(*) AS negativos
FROM fact_Invoices
WHERE Net_Invoice_Value < 0;