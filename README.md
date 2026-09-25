# Databricks Ecommerce Lakehouse

## Findings from Data Exploration
- Duplicate `review_id` values exist in `order_reviews` and duplicate keys exist in `geolocation`.
- Product dimensions/attributes have misspelled columns (e.g., `product_description_lenght`, `product_name_lenght`).
- `geolocation` has many rows per zip code prefix (not a unique primary key).
- The `order_reviews` dataset contains multi-line string comments, requiring `multiLine=True` when reading.
- Some payment values might be 0, requiring business rules for handling free orders or vouchers.
- Referential integrity is generally high, but missing states require handling.
