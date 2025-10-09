# TinyGoDBLoader

A lightweight Go CLI program to populate a database with metadata (categories, subcategories, and tags) for load testing.

## Features

- **Split Import Modes**: Import categories, subcategories, or tags separately
- **Bulk Inserts**: Tags are inserted in batches of 1000 for optimal performance
- **Idempotent**: Categories and subcategories don't duplicate; tags handle conflicts gracefully
- **Progress Bar**: Visual feedback during import
- **Simple**: Hardcoded data for one-time use

## Prerequisites

- Go 1.21 or higher
- PostgreSQL database
- Database schema created (see `schama.sql`)

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

### Import Categories

```bash
./tiny-cds-loader \
  -mode=categories \
  -db-url="postgres://localhost:5432/your_database" \
  -username="your_username" \
  -password="your_password" \
  -schema="public"
```

### Import Subcategories

```bash
./tiny-cds-loader \
  -mode=subcategories \
  -db-url="postgres://localhost:5432/your_database" \
  -username="your_username" \
  -password="your_password" \
  -schema="public"
```

### Import Tags

```bash
./tiny-cds-loader \
  -mode=tags \
  -db-url="postgres://localhost:5432/your_database" \
  -username="your_username" \
  -password="your_password" \
  -schema="public"
```

### CLI Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `-mode` | Yes | Operation mode: `categories`, `subcategories`, or `tags` | `categories` |
| `-db-url` | Yes | Database connection URL | `postgres://localhost:5432/mydb` |
| `-username` | Yes | Database username | `postgres` |
| `-password` | Yes | Database password | `secret123` |
| `-schema` | No | Target schema (default: "public") | `public` |

## Data

The tool can import:

- **9 Categories** from `categories.csv`
- **50 Subcategories** from `sub_categories.csv` (top 50 by product count)
- **12,000,000 Tags** (generated with random slugs)

All data is hardcoded or generated in the `main.go` file for simplicity. The slug is reused for category text fields (default_name, default_description, url_path). Tags are generated dynamically with random word combinations and inserted in batches of 1000 for optimal performance.

## Database Schema

The tool expects the following tables to exist:

- **`category`** table with columns: `category_id`, `parent_category_id`, `default_name`, `default_description`, `url_path`, `created_at`, `updated_at`
- **`tag`** table with columns: `tag_id`, `slug`, `in_landing_page`, `category`, `curated`

See `schama.sql` for the complete schema definition.

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

Importing 50 subcategories...
Subcategories [========================================] 50/50
  ✓ Inserted: 50, Skipped: 0

✓ Subcategories import completed successfully!
```

### Tags Mode

```
✓ Connected to database

=== Importing Tags ===

Importing 12000000 tags in batches of 1000...
Tags [========================================] 12000000/12000000
  ✓ Inserted: 12000000 tags

✓ Tags import completed successfully!
```

## Notes

- The import is **idempotent** for categories and subcategories - running it multiple times won't create duplicates
- Tags use bulk inserts with `ON CONFLICT DO NOTHING` to handle duplicates efficiently
- Categories and subcategories are stored in the same `category` table (subcategories have a `parent_category_id`)
- Run imports in order: **categories** → **subcategories** → **tags**
- Tags are inserted in batches of 1000 for optimal performance
- This tool is designed for one-time use with hardcoded data

## License

MIT
