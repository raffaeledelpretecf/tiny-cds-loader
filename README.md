# TinyGoDBLoader

A lightweight Go CLI program to populate a PostgreSQL database with realistic test data for load testing and performance benchmarking.

## Features

- **6 Import Modes**: Import categories, subcategories, tags, products, promos, and downloads independently
- **High-Performance Parallel Processing**: Uses 20 concurrent workers for optimal throughput
- **Bulk Inserts**: Optimized batch sizes (10,000 for tags, 4,000 for products)
- **Realistic Data Generation**: Creates products with proper relationships to categories, subcategories, and tags
- **Weighted Distribution**: Products follow realistic category distribution percentages
- **Progress Bars**: Real-time visual feedback during imports
- **Idempotent**: Safe to re-run without duplicating data

## Prerequisites

- Go 1.21 or higher
- PostgreSQL 16 with partitioning support
- Database schema created (see `schama.sql`)
- Required PostgreSQL extensions: `ltree`, `pg_partman`

## Installation

1. Clone or download this repository
2. Install dependencies:

```bash
go mod download
```

3. Build the binary:

```bash
go build -o tiny-cds-loader
```

## Usage

### 1. Import Categories

```bash
./tiny-cds-loader \
  -mode=categories \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### 2. Import Subcategories

```bash
./tiny-cds-loader \
  -mode=subcategories \
  -count=1000 \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### 3. Import Tags

```bash
./tiny-cds-loader \
  -mode=tags \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### 4. Import Products

```bash
./tiny-cds-loader \
  -mode=products \
  -count=100000 \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### 5. Import Product Promos

```bash
./tiny-cds-loader \
  -mode=promos \
  -count=5000 \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### 6. Import Product Downloads

```bash
./tiny-cds-loader \
  -mode=downloads \
  -count=1000000 \
  -db-url="postgres://localhost:5432/cds" \
  -username="admin" \
  -password="admin" \
  -schema="public"
```

### CLI Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `-mode` | Yes | Operation mode: `categories`, `subcategories`, `tags`, `products`, `promos`, or `downloads` | `products` |
| `-count` | Conditional | Number of records to insert (required for `subcategories`, `products`, `promos`, `downloads`) | `100000` |
| `-db-url` | Yes | Database connection URL | `postgres://localhost:5432/cds` |
| `-username` | Yes | Database username | `admin` |
| `-password` | Yes | Database password | `admin` |
| `-schema` | No | Target schema (default: "public") | `public` |

## Data

The tool can import:

- **9 Categories**: Hardcoded based on realistic product catalog (Graphics, Fonts, Crafts, etc.)
- **Custom Subcategories**: Dynamically generated with configurable count, randomly assigned to parent categories
- **12,000,000 Tags**: Generated with random adjective-noun combinations
- **Custom Products**: Configurable count with realistic relationships:
  - Each product assigned to 1 category (following weighted distribution)
  - Each product linked to 1-3 subcategories
  - Each product tagged with 20-30 random tags
- **Custom Promos**: Configurable count with types (discount, featured, bundle, seasonal, flash-sale) and statuses
- **Custom Downloads**: Configurable count distributed across last 14 days with hourly precision

All data is generated dynamically in `main.go` for maximum flexibility.

## Database Schema

The tool expects the following tables:

- **`category`**: Categories and subcategories (hierarchical)
- **`tag`**: Tags for product classification
- **`product`**: Main product table (partitioned by `category_id`)
- **`product_product_category`**: Product-subcategory relationships
- **`product_tag`**: Product-tag relationships  
- **`product_promo`**: Product promotions
- **`product_download`**: Download tracking records

See `schama.sql` for the complete schema with partitioning and indexes.

## Example Output

### Categories Mode

```
✓ Connected to database

=== Importing Categories ===

Importing 9 categories...
Categories [========================================] 9/9
  ✓ Inserted: 9, Skipped: 0

✓ Categories import completed successfully!
```

### Subcategories Mode

```
✓ Connected to database

=== Importing Subcategories ===

Importing 1000 subcategories...
Found 9 parent categories
Subcategories [========================================] 1000/1000
  ✓ Inserted: 1000 subcategories

✓ Subcategories import completed successfully!
```

### Tags Mode

```
✓ Connected to database

=== Importing Tags ===

Importing 12000000 tags in batches of 10000 using 20 workers...
Tags [========================================] 12000000/12000000
  ✓ Inserted: 12000000 tags

✓ Tags import completed successfully!
```

### Products Mode

```
✓ Connected to database

=== Importing Products ===

Importing 100000 products using 20 workers...
Loading subcategories from database...
Loaded 1000 subcategories
Products [========================================] 100000/100000
  ✓ Inserted: 100000 products

✓ Products import completed successfully!
```

### Promos Mode

```
✓ Connected to database

=== Importing Product Promos ===

Importing 5000 promos using 20 workers...
Found 100000 products in database
Promos [========================================] 5000/5000
  ✓ Inserted: 5000 promos

✓ Promos import completed successfully!
```

### Downloads Mode

```
✓ Connected to database

=== Importing Product Downloads ===

Importing 1000000 downloads using 20 workers...
Found 100000 products in database
Pre-generating timestamps...
Generated 336 unique hourly timestamps
Downloads [========================================] 1000000/1000000
  ✓ Inserted: 1000000 downloads

✓ Downloads import completed successfully!
```

## Notes

- **Import Order**: Run imports in this sequence:
  1. `categories` (creates 9 base categories)
  2. `subcategories` (requires categories to exist)
  3. `tags` (can run independently)
  4. `products` (requires categories, subcategories, and tags)
  5. `promos` (requires products)
  6. `downloads` (requires products)

- **Performance**:
  - Uses 20 parallel workers for high throughput
  - Optimized batch sizes: 10,000 (tags), 4,000 (products), 5,000 (downloads)
  - Connection pool: 50 max connections, 25 idle connections

- **Data Characteristics**:
  - Products follow weighted category distribution (93.2% Graphics, 2.1% Fonts, etc.)
  - Each product has 20-30 tags on average
  - Downloads span last 14 days with hourly precision
  - Promos include 5 types and 4 statuses with realistic expiration dates

- **Idempotency**:
  - Categories and subcategories use `ON CONFLICT DO NOTHING`
  - Tags handle duplicates gracefully
  - Products mode is **append-only** - can be run multiple times to add more products

- **Database Requirements**:
  - Product table is partitioned by `category_id` (9 partitions)
  - Requires PostgreSQL extensions: `ltree`, `pg_partman`

## Docker Setup

A `docker-compose.yml` is included for local testing:

```bash
# Start PostgreSQL
docker-compose up -d

# Create schema
psql -h localhost -U admin -d cds -f schama.sql

# Run imports
./tiny-cds-loader -mode=categories -db-url="postgres://localhost:5432/cds" -username="admin" -password="admin"
```

## Performance Benchmarks

Typical import times on modern hardware:

- **Categories**: < 1 second (9 records)
- **Subcategories**: ~1 second (1,000 records)
- **Tags**: ~5-10 minutes (12,000,000 records)
- **Products**: ~2-5 minutes (100,000 records with relationships)
- **Promos**: ~10 seconds (5,000 records)
- **Downloads**: ~1-2 minutes (1,000,000 records)

## License

MIT
