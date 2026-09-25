-- Access model: analysts read gold only; engineers own all ecom schemas.
GRANT USE CATALOG ON CATALOG portfolio TO `ecom_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA portfolio.ecom_gold TO `ecom_analysts`;

GRANT USE CATALOG ON CATALOG portfolio TO `ecom_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA portfolio.ecom_bronze TO `ecom_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA portfolio.ecom_silver TO `ecom_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA portfolio.ecom_gold TO `ecom_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA portfolio.ecom_ops TO `ecom_engineers`;

SHOW GRANTS ON SCHEMA portfolio.ecom_gold;
