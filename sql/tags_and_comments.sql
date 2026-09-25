-- Run in the SQL editor. If ALTER is refused on pipeline-managed tables, set comments in the
-- pipeline code instead and add tags in Catalog Explorer.
COMMENT ON TABLE portfolio.ecom_gold.agg_daily_sales
  IS 'Daily delivered revenue, orders and late-delivery rate. Refresh: every run.';
ALTER TABLE portfolio.ecom_gold.agg_daily_sales SET TAGS ('layer' = 'gold', 'domain' = 'sales');
ALTER TABLE portfolio.ecom_gold.fact_order_item SET TAGS ('layer' = 'gold', 'domain' = 'sales');
ALTER TABLE portfolio.ecom_gold.dim_customer SET TAGS ('layer' = 'gold', 'domain' = 'customer');
ALTER TABLE portfolio.ecom_gold.dim_customer ALTER COLUMN city SET TAGS ('pii' = 'true');
ALTER TABLE portfolio.ecom_gold.dim_customer ALTER COLUMN zip_code_prefix SET TAGS ('pii' = 'true');
