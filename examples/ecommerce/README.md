# Ecommerce Example

A comprehensive example demonstrating `pgcrate` features with an e-commerce domain.

## Features Demonstrated

- **Migrations**: Tables for customers, products, orders, inventory
- **Models**: Sales analytics, customer metrics, inventory alerts
- **Seeds**: Reference data for categories, shipping methods
- **Sequences**: Order numbers, product SKUs (useful for `pgcrate sequences` diagnostics)
- **Foreign Keys**: Relationships between tables (useful for `pgcrate indexes` diagnostics)

## Quick Start

```bash
# Navigate to example
cd examples/ecommerce

# Set database URL
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/ecommerce_dev"

# Create database (if needed)
createdb ecommerce_dev

# Apply migrations
pgcrate migrate up

# Load seed data
pgcrate seed run

# Run analytics models
pgcrate model run

# Check database health
pgcrate doctor
pgcrate sequences
pgcrate indexes
```

## Domain Model

**Core tables:**
- `customers` - Customer accounts with email, name
- `products` - Product catalog with SKU, price, stock
- `categories` - Product categories
- `orders` - Customer orders with status tracking
- `order_items` - Line items for each order
- `inventory_movements` - Stock changes (in/out)

**Reference data (seeds):**
- `order_statuses` - Valid order statuses (pending, confirmed, shipped, delivered, cancelled)
- `shipping_methods` - Available shipping options
- `countries` - Supported shipping countries

**Analytics models:**
- `staging/stg_orders` - Clean order data
- `staging/stg_customers` - Clean customer data
- `marts/customer_metrics` - Customer lifetime value, order count
- `marts/product_sales` - Product performance metrics
- `marts/daily_revenue` - Daily revenue rollup

## Project Structure

```
ecommerce/
├── pgcrate.toml              # Main configuration
├── README.md
├── db/
│   └── migrations/
│       ├── 20250101000000_create_extensions.sql
│       ├── 20250101001000_create_reference_tables.sql
│       ├── 20250101002000_create_customers.sql
│       ├── 20250101003000_create_products.sql
│       ├── 20250101004000_create_orders.sql
│       └── 20250101005000_create_inventory.sql
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   └── marts/
│       ├── customer_metrics.sql
│       ├── product_sales.sql
│       └── daily_revenue.sql
└── seeds/
    └── public/
        ├── order_statuses.csv
        ├── shipping_methods.csv
        └── countries.csv
```

## Diagnostic Examples

This example is designed to work well with pgcrate's diagnostic commands:

```bash
# Check sequence health (order_number_seq, product_sku_seq)
pgcrate sequences

# Check for missing/duplicate indexes on foreign keys
pgcrate indexes

# Check for blocking locks during heavy order processing
pgcrate locks

# Overall health
pgcrate doctor
```

## Testing with Sample Data

The seeds provide reference data. To add sample transactional data:

```sql
-- Add sample customer
INSERT INTO customers (email, first_name, last_name, country_code)
VALUES ('jane@example.com', 'Jane', 'Doe', 'US');

-- Add sample product
INSERT INTO products (sku, name, category_id, price, stock_quantity)
VALUES ('WIDGET-001', 'Premium Widget', 1, 49.99, 100);

-- Create an order
INSERT INTO orders (customer_id, shipping_method_id, status)
VALUES (1, 1, 'pending');

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES (1, 1, 2, 49.99);
```
