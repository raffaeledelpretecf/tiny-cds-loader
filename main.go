package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
)

// Category represents a product category
type Category struct {
	ID         int64
	Slug       string
	Percentage float64
}

// Subcategory represents a product subcategory
type Subcategory struct {
	ID               int64
	Slug             string
	ParentCategoryID int64
	Percentage       float64
}

const (
	totalTags         = 12000000 // 12 million tags as per specs
	batchSize         = 10000    // Insert tags in batches (limited by PostgreSQL's 65535 parameter limit)
	numWorkers        = 20       // Number of parallel workers (increased for better throughput)
	productBatchSize  = 4000     // Products per batch (4000 * 15 params = 60,000 < 65,535 limit)
	avgTagsPerProduct = 25       // Average tags per product
)

// Word lists for generating random tag slugs
var adjectives = []string{
	"abstract", "ancient", "artistic", "beautiful", "bold", "bright", "classic", "clean",
	"colorful", "creative", "cute", "dark", "decorative", "delicate", "elegant", "fancy",
	"festive", "floral", "fresh", "fun", "geometric", "golden", "gorgeous", "graceful",
	"handmade", "happy", "luxury", "magical", "minimal", "modern", "natural", "organic",
	"ornate", "playful", "premium", "pretty", "retro", "rustic", "seasonal", "simple",
	"stylish", "trendy", "unique", "urban", "vibrant", "vintage", "wild", "wonderful",
}

var nouns = []string{
	"art", "background", "badge", "banner", "border", "bouquet", "card", "celebration",
	"collection", "decoration", "design", "drawing", "element", "emblem", "flower", "frame",
	"graphic", "icon", "illustration", "image", "label", "layout", "logo", "ornament",
	"pattern", "poster", "print", "set", "shape", "sign", "silhouette", "sketch",
	"sticker", "style", "symbol", "template", "texture", "theme", "vector", "wallpaper",
	"watercolor", "wreath", "bundle", "pack", "kit", "clipart", "mockup", "scene",
}

// Hardcoded categories from categories.csv
var categories = []Category{
	{ID: 553, Slug: "Graphics", Percentage: 0.9320},
	{ID: 23, Slug: "Fonts", Percentage: 0.0210},
	{ID: 26, Slug: "Crafts", Percentage: 0.0185},
	{ID: 735, Slug: "Embroidery", Percentage: 0.0098},
	{ID: 2245, Slug: "Laser Cutting", Percentage: 0.0091},
	{ID: 546, Slug: "Bundles", Percentage: 0.0065},
	{ID: 1850, Slug: "3D SVG", Percentage: 0.0029},
	{ID: 2244, Slug: "3D Printing", Percentage: 0.0003},
	{ID: 2246, Slug: "Knitting", Percentage: 0.0002},
}

// Hardcoded subcategories from sub_categories.csv (top 50 for simplicity)
var subcategories = []Subcategory{
	{ID: 1804, Slug: "Graphics", ParentCategoryID: 553, Percentage: 0.2723225},
	{ID: 638, Slug: "Crafts", ParentCategoryID: 26, Percentage: 0.1364864},
	{ID: 602, Slug: "Illustrations", ParentCategoryID: 553, Percentage: 0.1073622},
	{ID: 1281, Slug: "T-shirt Designs", ParentCategoryID: 553, Percentage: 0.0964248},
	{ID: 580, Slug: "Icons", ParentCategoryID: 553, Percentage: 0.0459113},
	{ID: 610, Slug: "Print Templates", ParentCategoryID: 553, Percentage: 0.0429839},
	{ID: 1841, Slug: "Transparent PNGs", ParentCategoryID: 553, Percentage: 0.0347678},
	{ID: 615, Slug: "Logos", ParentCategoryID: 553, Percentage: 0.0260298},
	{ID: 608, Slug: "Backgrounds", ParentCategoryID: 553, Percentage: 0.0245767},
	{ID: 1826, Slug: "Patterns", ParentCategoryID: 553, Percentage: 0.0211616},
	{ID: 604, Slug: "Patterns", ParentCategoryID: 553, Percentage: 0.0203491},
	{ID: 1854, Slug: "AI Illustrations", ParentCategoryID: 553, Percentage: 0.0192229},
	{ID: 634, Slug: "Tumbler Wraps", ParentCategoryID: 26, Percentage: 0.0166457},
	{ID: 897, Slug: "KDP Interiors", ParentCategoryID: 553, Percentage: 0.0086374},
	{ID: 609, Slug: "Graphic Templates", ParentCategoryID: 553, Percentage: 0.0083662},
	{ID: 1853, Slug: "AI Graphics", ParentCategoryID: 553, Percentage: 0.0080051},
	{ID: 605, Slug: "Product Mockups", ParentCategoryID: 553, Percentage: 0.0069095},
	{ID: 8, Slug: "Script & Handwritten", ParentCategoryID: 23, Percentage: 0.0061043},
	{ID: 2111, Slug: "T-Shirts", ParentCategoryID: 553, Percentage: 0.0052110},
	{ID: 1856, Slug: "AI Transparent PNGs", ParentCategoryID: 553, Percentage: 0.0051267},
	{ID: 908, Slug: "Coloring Pages & Books Adults", ParentCategoryID: 553, Percentage: 0.0037459},
	{ID: 606, Slug: "Textures", ParentCategoryID: 553, Percentage: 0.0036388},
	{ID: 1829, Slug: "AI Generated", ParentCategoryID: 553, Percentage: 0.0034918},
	{ID: 1858, Slug: "Coloring Pages", ParentCategoryID: 553, Percentage: 0.0032232},
	{ID: 611, Slug: "Product Mockups", ParentCategoryID: 553, Percentage: 0.0029800},
	{ID: 12, Slug: "Display", ParentCategoryID: 23, Percentage: 0.0029284},
	{ID: 584, Slug: "Layer Styles", ParentCategoryID: 553, Percentage: 0.0029192},
	{ID: 907, Slug: "Coloring Pages & Books Kids", ParentCategoryID: 553, Percentage: 0.0027455},
	{ID: 1833, Slug: "Sketches", ParentCategoryID: 553, Percentage: 0.0024499},
	{ID: 906, Slug: "Coloring Pages & Books", ParentCategoryID: 553, Percentage: 0.0019535},
	{ID: 2112, Slug: "Hoodies & Sweatshirts", ParentCategoryID: 553, Percentage: 0.0018835},
	{ID: 1280, Slug: "Social Media Templates", ParentCategoryID: 553, Percentage: 0.0016599},
	{ID: 1857, Slug: "AI Patterns", ParentCategoryID: 553, Percentage: 0.0013482},
	{ID: 2031, Slug: "Decorative Elements", ParentCategoryID: 553, Percentage: 0.0011899},
	{ID: 2167, Slug: "Mugs & Cups", ParentCategoryID: 553, Percentage: 0.0011692},
	{ID: 612, Slug: "Websites", ParentCategoryID: 553, Percentage: 0.0011454},
	{ID: 67, Slug: "Designs & Drawings", ParentCategoryID: 553, Percentage: 0.0011122},
	{ID: 617, Slug: "Presentation Templates", ParentCategoryID: 553, Percentage: 0.0010494},
	{ID: 13, Slug: "Sans Serif", ParentCategoryID: 23, Percentage: 0.0010465},
	{ID: 2169, Slug: "Frames & Posters", ParentCategoryID: 553, Percentage: 0.0010009},
	{ID: 581, Slug: "Add-ons", ParentCategoryID: 553, Percentage: 0.0009821},
	{ID: 582, Slug: "Actions & Presets", ParentCategoryID: 553, Percentage: 0.0009456},
	{ID: 2117, Slug: "Baby & Kids Clothing", ParentCategoryID: 553, Percentage: 0.0008957},
	{ID: 2357, Slug: "Wall Decor", ParentCategoryID: 26, Percentage: 0.0008954},
	{ID: 2223, Slug: "Christmas & New Year", ParentCategoryID: 553, Percentage: 0.0008319},
	{ID: 1145, Slug: "KDP Keywords", ParentCategoryID: 553, Percentage: 0.0008303},
	{ID: 2365, Slug: "Winter & Christmas", ParentCategoryID: 553, Percentage: 0.0007974},
	{ID: 14, Slug: "Serif", ParentCategoryID: 23, Percentage: 0.0007710},
	{ID: 27, Slug: "Christmas", ParentCategoryID: 26, Percentage: 0.0006991},
	{ID: 583, Slug: "Brushes", ParentCategoryID: 553, Percentage: 0.0006624},
}

func main() {
	// CLI flags
	mode := flag.String("mode", "", "Operation mode: 'categories', 'subcategories', 'tags', 'products', 'promos', or 'downloads'")
	dbURL := flag.String("db-url", "", "Database connection URL")
	username := flag.String("username", "", "Database username")
	password := flag.String("password", "", "Database password")
	schemaName := flag.String("schema", "public", "Target schema to populate")
	count := flag.Int("count", 0, "Number of records to insert (required for 'products', 'promos', and 'downloads' modes)")

	flag.Parse()

	// Validate required flags
	if *mode == "" {
		log.Fatal("Error: -mode flag is required (categories, subcategories, tags, products, promos, or downloads)")
	}

	if *dbURL == "" {
		log.Fatal("Error: -db-url flag is required")
	}

	if *username == "" {
		log.Fatal("Error: -username flag is required")
	}

	if *password == "" {
		log.Fatal("Error: -password flag is required")
	}

	// Validate mode
	validModes := map[string]bool{"categories": true, "subcategories": true, "tags": true, "products": true, "promos": true, "downloads": true}
	if !validModes[*mode] {
		log.Fatal("Error: mode must be one of: categories, subcategories, tags, products, promos, downloads")
	}

	// Validate count for products, promos, and downloads modes
	if (*mode == "products" || *mode == "promos" || *mode == "downloads") && *count <= 0 {
		log.Fatal("Error: -count flag is required and must be > 0 for 'products', 'promos', and 'downloads' modes")
	}

	// Build connection string
	connStr := fmt.Sprintf("%s?user=%s&password=%s&sslmode=disable", *dbURL, *username, *password)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Set connection pool settings for high throughput
	db.SetMaxOpenConns(50) // Allow up to 50 concurrent connections
	db.SetMaxIdleConns(25) // Keep 25 idle connections ready
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	fmt.Println("✓ Connected to database")

	// Set search path to schema
	if _, err := db.Exec(fmt.Sprintf("SET search_path TO %s", *schemaName)); err != nil {
		log.Fatalf("Failed to set schema: %v", err)
	}

	// Import based on mode
	switch *mode {
	case "categories":
		if err := importCategories(db); err != nil {
			log.Fatalf("Failed to import categories: %v", err)
		}
		fmt.Println("\n✓ Categories import completed successfully!")
	case "subcategories":
		if err := importSubcategories(db); err != nil {
			log.Fatalf("Failed to import subcategories: %v", err)
		}
		fmt.Println("\n✓ Subcategories import completed successfully!")
	case "tags":
		if err := importTags(db); err != nil {
			log.Fatalf("Failed to import tags: %v", err)
		}
		fmt.Println("\n✓ Tags import completed successfully!")
	case "products":
		if err := importProducts(db, *count); err != nil {
			log.Fatalf("Failed to import products: %v", err)
		}
		fmt.Println("\n✓ Products import completed successfully!")
	case "promos":
		if err := importPromos(db, *count); err != nil {
			log.Fatalf("Failed to import promos: %v", err)
		}
		fmt.Println("\n✓ Promos import completed successfully!")
	case "downloads":
		if err := importDownloads(db, *count); err != nil {
			log.Fatalf("Failed to import downloads: %v", err)
		}
		fmt.Println("\n✓ Downloads import completed successfully!")
	}
}

func importCategories(db *sql.DB) error {
	fmt.Println("\n=== Importing Categories ===\n")
	fmt.Printf("Importing %d categories...\n", len(categories))

	bar := progressbar.NewOptions(len(categories),
		progressbar.OptionSetDescription("Categories"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	inserted := 0
	skipped := 0

	for _, cat := range categories {
		// Check if category already exists
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM category WHERE category_id = $1)", cat.ID).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check category existence: %w", err)
		}

		if !exists {
			// Insert category
			_, err = db.Exec(`
				INSERT INTO category (
					category_id, 
					parent_category_id, 
					default_name, 
					default_description, 
					url_path,
					created_at,
					updated_at
				) VALUES ($1, NULL, $2, $3, $4, $5, $6)
			`, cat.ID, cat.Slug, fmt.Sprintf("Description for %s", cat.Slug), cat.Slug, time.Now(), time.Now())

			if err != nil {
				return fmt.Errorf("failed to insert category %d: %w", cat.ID, err)
			}
			inserted++
		} else {
			skipped++
		}

		bar.Add(1)
	}

	fmt.Printf("\n  ✓ Inserted: %d, Skipped: %d\n\n", inserted, skipped)
	return nil
}

func importSubcategories(db *sql.DB) error {
	fmt.Println("\n=== Importing Subcategories ===\n")
	fmt.Printf("Importing %d subcategories...\n", len(subcategories))

	bar := progressbar.NewOptions(len(subcategories),
		progressbar.OptionSetDescription("Subcategories"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	inserted := 0
	skipped := 0

	for _, subcat := range subcategories {
		// Check if subcategory already exists
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM category WHERE category_id = $1)", subcat.ID).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check subcategory existence: %w", err)
		}

		if !exists {
			// Insert subcategory
			_, err = db.Exec(`
				INSERT INTO category (
					category_id, 
					parent_category_id, 
					default_name, 
					default_description, 
					url_path,
					created_at,
					updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7)
			`, subcat.ID, subcat.ParentCategoryID, subcat.Slug, fmt.Sprintf("Description for %s", subcat.Slug), subcat.Slug, time.Now(), time.Now())

			if err != nil {
				return fmt.Errorf("failed to insert subcategory %d: %w", subcat.ID, err)
			}
			inserted++
		} else {
			skipped++
		}

		bar.Add(1)
	}

	fmt.Printf("\n  ✓ Inserted: %d, Skipped: %d\n\n", inserted, skipped)
	return nil
}

func importTags(db *sql.DB) error {
	fmt.Println("\n=== Importing Tags ===\n")
	fmt.Printf("Importing %d tags in batches of %d using %d workers...\n", totalTags, batchSize, numWorkers)

	bar := progressbar.NewOptions(totalTags,
		progressbar.OptionSetDescription("Tags"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Create work channel and error channel
	type batchJob struct {
		start int
		end   int
	}

	jobs := make(chan batchJob, 100)
	errors := make(chan error, numWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	totalInserted := 0

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker has its own random generator
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for job := range jobs {
				batchCount := job.end - job.start + 1

				// Begin transaction for batch
				tx, err := db.Begin()
				if err != nil {
					errors <- fmt.Errorf("worker %d: failed to begin transaction: %w", workerID, err)
					continue
				}

				// Build multi-value INSERT statement
				var queryBuilder strings.Builder
				valueArgs := make([]interface{}, 0, batchCount*5)

				queryBuilder.WriteString("INSERT INTO tag (tag_id, slug, in_landing_page, category, curated) VALUES ")

				argPos := 1
				for i := job.start; i <= job.end; i++ {
					tagID := int64(i)
					slug := generateRandomTagSlug(rng)

					if i > job.start {
						queryBuilder.WriteString(", ")
					}
					queryBuilder.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
						argPos, argPos+1, argPos+2, argPos+3, argPos+4))
					valueArgs = append(valueArgs, tagID, slug, false, false, false)
					argPos += 5
				}

				queryBuilder.WriteString(" ON CONFLICT (tag_id) DO NOTHING")

				_, err = tx.Exec(queryBuilder.String(), valueArgs...)
				if err != nil {
					tx.Rollback()
					errors <- fmt.Errorf("worker %d: failed to insert batch starting at %d: %w", workerID, job.start, err)
					continue
				}

				// Commit transaction
				if err := tx.Commit(); err != nil {
					errors <- fmt.Errorf("worker %d: failed to commit transaction: %w", workerID, err)
					continue
				}

				// Update progress
				mu.Lock()
				totalInserted += batchCount
				bar.Add(batchCount)
				mu.Unlock()
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		for batchStart := 1; batchStart <= totalTags; batchStart += batchSize {
			batchEnd := batchStart + batchSize - 1
			if batchEnd > totalTags {
				batchEnd = totalTags
			}
			jobs <- batchJob{start: batchStart, end: batchEnd}
		}
		close(jobs)
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	fmt.Printf("\n  ✓ Inserted: %d tags\n\n", totalInserted)
	return nil
}

func generateRandomTagSlug(rng *rand.Rand) string {
	// Generate random combinations of adjective + noun, or just noun
	if rng.Intn(2) == 0 {
		// adjective + noun
		adj := adjectives[rng.Intn(len(adjectives))]
		noun := nouns[rng.Intn(len(nouns))]
		return fmt.Sprintf("%s-%s", adj, noun)
	}
	// just noun
	return nouns[rng.Intn(len(nouns))]
}

func importProducts(db *sql.DB, productCount int) error {
	fmt.Println("\n=== Importing Products ===\n")
	fmt.Printf("Importing %d products using %d workers...\n", productCount, numWorkers)

	// Get the starting product ID by finding the max existing product_id
	var startID int64
	err := db.QueryRow("SELECT COALESCE(MAX(product_id), 0) FROM product").Scan(&startID)
	if err != nil {
		return fmt.Errorf("failed to get starting product ID: %w", err)
	}
	startID++ // Start from next available ID

	// Build weighted category distribution
	categoryWeights := buildCategoryWeights()
	subcategoryWeights := buildSubcategoryWeights()

	bar := progressbar.NewOptions(productCount,
		progressbar.OptionSetDescription("Products"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Create work channel
	type productBatch struct {
		startID int64
		count   int
	}

	jobs := make(chan productBatch, 100)
	errors := make(chan error, 1000) // Larger buffer for errors
	var wg sync.WaitGroup
	var mu sync.Mutex
	totalInserted := 0
	var firstError error

	// Error collector goroutine
	var errorWg sync.WaitGroup
	errorWg.Add(1)
	go func() {
		defer errorWg.Done()
		for err := range errors {
			if err != nil && firstError == nil {
				mu.Lock()
				if firstError == nil {
					firstError = err
				}
				mu.Unlock()
			}
		}
	}()

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))

			for batch := range jobs {
				if err := insertProductBatch(db, batch.startID, batch.count, categoryWeights, subcategoryWeights, rng); err != nil {
					errors <- fmt.Errorf("worker %d: %w", workerID, err)
					continue
				}

				mu.Lock()
				totalInserted += batch.count
				bar.Add(batch.count)
				mu.Unlock()
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		currentID := startID
		remaining := productCount

		for remaining > 0 {
			count := productBatchSize
			if remaining < productBatchSize {
				count = remaining
			}

			jobs <- productBatch{startID: currentID, count: count}
			currentID += int64(count)
			remaining -= count
		}
		close(jobs)
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(errors)

	// Wait for error collector to finish
	errorWg.Wait()

	// Check if there was an error
	if firstError != nil {
		return firstError
	}

	fmt.Printf("\n  ✓ Inserted: %d products\n\n", totalInserted)
	return nil
}

func buildCategoryWeights() []float64 {
	weights := make([]float64, len(categories))
	sum := 0.0
	for i, cat := range categories {
		sum += cat.Percentage
		weights[i] = sum
	}
	return weights
}

func buildSubcategoryWeights() map[int64][]float64 {
	weights := make(map[int64][]float64)

	for _, cat := range categories {
		var subWeights []float64
		sum := 0.0

		for _, subcat := range subcategories {
			if subcat.ParentCategoryID == cat.ID {
				sum += subcat.Percentage
				subWeights = append(subWeights, sum)
			}
		}

		if len(subWeights) > 0 {
			weights[cat.ID] = subWeights
		}
	}

	return weights
}

func selectCategoryByWeight(rng *rand.Rand, weights []float64) int64 {
	r := rng.Float64()
	for i, w := range weights {
		if r <= w {
			return categories[i].ID
		}
	}
	return categories[len(categories)-1].ID
}

func selectSubcategoriesByWeight(rng *rand.Rand, categoryID int64, weights map[int64][]float64) []int64 {
	subWeights, exists := weights[categoryID]
	if !exists || len(subWeights) == 0 {
		return []int64{}
	}

	// Select 1-3 subcategories for this category
	numSubs := rng.Intn(3) + 1
	selected := make(map[int64]bool)
	result := []int64{}

	for i := 0; i < numSubs && len(result) < len(subWeights); i++ {
		r := rng.Float64()
		for j, w := range subWeights {
			if r <= w {
				// Find the actual subcategory at this index for this category
				idx := 0
				for _, subcat := range subcategories {
					if subcat.ParentCategoryID == categoryID {
						if idx == j && !selected[subcat.ID] {
							selected[subcat.ID] = true
							result = append(result, subcat.ID)
							break
						}
						idx++
					}
				}
				break
			}
		}
	}

	return result
}

func selectRandomTags(rng *rand.Rand) []int64 {
	// Generate ~25 tags per product (with some variance)
	numTags := avgTagsPerProduct + rng.Intn(11) - 5 // 20-30 tags
	if numTags < 1 {
		numTags = 1
	}

	tags := make([]int64, numTags)
	for i := 0; i < numTags; i++ {
		tags[i] = int64(rng.Intn(totalTags) + 1)
	}
	return tags
}

func insertProductBatch(db *sql.DB, startID int64, count int, categoryWeights []float64, subcategoryWeights map[int64][]float64, rng *rand.Rand) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build batch insert for products
	var productQuery strings.Builder
	productQuery.WriteString("INSERT INTO product (product_id, author_id, category_id, price_in_cents, title, slug, description, main_image, images, assets, product_type, product_status, metadata, created_at, status) VALUES ")

	productArgs := make([]interface{}, 0, count*15)

	type productRelation struct {
		productID      int64
		categoryID     int64
		subcategoryIDs []int64
		tagIDs         []int64
	}

	relations := make([]productRelation, 0, count)

	for i := 0; i < count; i++ {
		productID := startID + int64(i)
		categoryID := selectCategoryByWeight(rng, categoryWeights)
		subcategoryIDs := selectSubcategoriesByWeight(rng, categoryID, subcategoryWeights)
		tagIDs := selectRandomTags(rng)

		relations = append(relations, productRelation{
			productID:      productID,
			categoryID:     categoryID,
			subcategoryIDs: subcategoryIDs,
			tagIDs:         tagIDs,
		})

		if i > 0 {
			productQuery.WriteString(", ")
		}

		argPos := i*15 + 1
		productQuery.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argPos, argPos+1, argPos+2, argPos+3, argPos+4, argPos+5, argPos+6, argPos+7, argPos+8, argPos+9, argPos+10, argPos+11, argPos+12, argPos+13, argPos+14))

		// Generate mock data
		title := fmt.Sprintf("Product %d - %s %s", productID, adjectives[rng.Intn(len(adjectives))], nouns[rng.Intn(len(nouns))])
		slug := fmt.Sprintf("product-%d", productID)

		productArgs = append(productArgs,
			productID,
			int64(rng.Intn(10000)+1), // author_id
			categoryID,
			int64(rng.Intn(10000)+99), // price_in_cents
			fmt.Sprintf(`{"en": "%s"}`, title),
			fmt.Sprintf(`{"en": "%s"}`, slug),
			fmt.Sprintf(`{"en": "Description for %s"}`, title),
			`{}`, // main_image
			`[]`, // images
			`[]`, // assets
			"digital",
			"published",
			`{}`, // metadata
			time.Now(),
			"publish", // status
		)
	}

	// Insert products
	_, err = tx.Exec(productQuery.String(), productArgs...)
	if err != nil {
		return fmt.Errorf("failed to insert products: %w", err)
	}

	// Insert product_product_category relations
	if len(relations) > 0 {
		var categoryQuery strings.Builder
		categoryQuery.WriteString("INSERT INTO product_product_category (product_id, category_id) VALUES ")
		categoryArgs := make([]interface{}, 0)
		argPos := 1
		first := true

		for _, rel := range relations {
			for _, subcatID := range rel.subcategoryIDs {
				if !first {
					categoryQuery.WriteString(", ")
				}
				first = false
				categoryQuery.WriteString(fmt.Sprintf("($%d, $%d)", argPos, argPos+1))
				categoryArgs = append(categoryArgs, rel.productID, subcatID)
				argPos += 2
			}
		}

		if len(categoryArgs) > 0 {
			categoryQuery.WriteString(" ON CONFLICT DO NOTHING")
			_, err = tx.Exec(categoryQuery.String(), categoryArgs...)
			if err != nil {
				return fmt.Errorf("failed to insert product categories: %w", err)
			}
		}
	}

	// Insert product_tag relations in smaller sub-batches to avoid parameter limit
	const tagBatchSize = 10000 // Max tags per insert
	allTagArgs := make([]interface{}, 0)

	for _, rel := range relations {
		for _, tagID := range rel.tagIDs {
			allTagArgs = append(allTagArgs, rel.productID, tagID)
		}
	}

	for i := 0; i < len(allTagArgs); i += tagBatchSize * 2 {
		end := i + tagBatchSize*2
		if end > len(allTagArgs) {
			end = len(allTagArgs)
		}

		batchArgs := allTagArgs[i:end]
		if len(batchArgs) == 0 {
			continue
		}

		var tagQuery strings.Builder
		tagQuery.WriteString("INSERT INTO product_tag (product_id, tag_id) VALUES ")

		argPos := 1
		for j := 0; j < len(batchArgs)/2; j++ {
			if j > 0 {
				tagQuery.WriteString(", ")
			}
			tagQuery.WriteString(fmt.Sprintf("($%d, $%d)", argPos, argPos+1))
			argPos += 2
		}

		tagQuery.WriteString(" ON CONFLICT DO NOTHING")
		_, err = tx.Exec(tagQuery.String(), batchArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert product tags: %w", err)
		}
	}

	return tx.Commit()
}

func importPromos(db *sql.DB, promoCount int) error {
	fmt.Println("\n=== Importing Product Promos ===\n")
	fmt.Printf("Importing %d promos using %d workers...\n", promoCount, numWorkers)

	// Get total product count
	var totalProducts int64
	err := db.QueryRow("SELECT COUNT(*) FROM product").Scan(&totalProducts)
	if err != nil {
		return fmt.Errorf("failed to count products: %w", err)
	}

	if totalProducts == 0 {
		return fmt.Errorf("no products found in database - please import products first")
	}

	fmt.Printf("Found %d products in database\n", totalProducts)

	// Get the starting promo ID
	var startPromoID int64
	err = db.QueryRow("SELECT COALESCE(MAX(product_promo_id), 0) FROM product_promo").Scan(&startPromoID)
	if err != nil {
		return fmt.Errorf("failed to get starting promo ID: %w", err)
	}
	startPromoID++ // Start from next available ID

	bar := progressbar.NewOptions(promoCount,
		progressbar.OptionSetDescription("Promos"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowCount(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Create work channel
	type promoBatch struct {
		startPromoID int64
		count        int
	}

	jobs := make(chan promoBatch, 100)
	errors := make(chan error, 1000)
	var wg sync.WaitGroup
	var mu sync.Mutex
	totalInserted := 0
	var firstError error

	// Error collector goroutine
	var errorWg sync.WaitGroup
	errorWg.Add(1)
	go func() {
		defer errorWg.Done()
		for err := range errors {
			if err != nil && firstError == nil {
				mu.Lock()
				if firstError == nil {
					firstError = err
				}
				mu.Unlock()
			}
		}
	}()

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))

			for batch := range jobs {
				if err := insertPromoBatch(db, batch.startPromoID, batch.count, totalProducts, rng); err != nil {
					errors <- fmt.Errorf("worker %d: %w", workerID, err)
					continue
				}

				mu.Lock()
				totalInserted += batch.count
				bar.Add(batch.count)
				mu.Unlock()
			}
		}(w)
	}

	// Send jobs to workers
	go func() {
		currentPromoID := startPromoID
		remaining := promoCount
		batchSize := 1000 // Smaller batches for promos

		for remaining > 0 {
			count := batchSize
			if remaining < batchSize {
				count = remaining
			}

			jobs <- promoBatch{startPromoID: currentPromoID, count: count}
			currentPromoID += int64(count)
			remaining -= count
		}
		close(jobs)
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(errors)

	// Wait for error collector to finish
	errorWg.Wait()

	// Check if there was an error
	if firstError != nil {
		return firstError
	}

	fmt.Printf("\n  ✓ Inserted: %d promos\n\n", totalInserted)
	return nil
}

func insertPromoBatch(db *sql.DB, startPromoID int64, count int, totalProducts int64, rng *rand.Rand) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get random product IDs
	productIDs := make([]int64, count)
	rows, err := tx.Query("SELECT product_id FROM product ORDER BY RANDOM() LIMIT $1", count)
	if err != nil {
		return fmt.Errorf("failed to select random products: %w", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() && i < count {
		if err := rows.Scan(&productIDs[i]); err != nil {
			return fmt.Errorf("failed to scan product ID: %w", err)
		}
		i++
	}
	rows.Close()

	if i < count {
		// Not enough unique products, fill remaining with random IDs
		for ; i < count; i++ {
			productIDs[i] = int64(rng.Int63n(totalProducts) + 1)
		}
	}

	// Promo types and statuses
	promoTypes := []string{"discount", "featured", "bundle", "seasonal", "flash-sale"}
	statuses := []string{"active", "scheduled", "expired", "paused"}

	// Build batch insert for promos
	var promoQuery strings.Builder
	promoQuery.WriteString("INSERT INTO product_promo (product_promo_id, product_id, promo_type, status, expires_at, created_at, last_updated_at) VALUES ")

	promoArgs := make([]interface{}, 0, count*7)
	now := time.Now()

	for i := 0; i < count; i++ {
		if i > 0 {
			promoQuery.WriteString(", ")
		}

		promoID := startPromoID + int64(i)
		productID := productIDs[i]
		promoType := promoTypes[rng.Intn(len(promoTypes))]
		status := statuses[rng.Intn(len(statuses))]

		// Generate expiration date: 1 to 90 days from now
		daysUntilExpiry := rng.Intn(90) + 1
		expiresAt := now.AddDate(0, 0, daysUntilExpiry)

		// Created at: within last 30 days
		daysAgo := rng.Intn(30)
		createdAt := now.AddDate(0, 0, -daysAgo)

		argPos := i*7 + 1
		promoQuery.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argPos, argPos+1, argPos+2, argPos+3, argPos+4, argPos+5, argPos+6))

		promoArgs = append(promoArgs,
			promoID,
			productID,
			promoType,
			status,
			expiresAt,
			createdAt,
			now,
		)
	}

	promoQuery.WriteString(" ON CONFLICT (product_id) DO UPDATE SET promo_type = EXCLUDED.promo_type, status = EXCLUDED.status, expires_at = EXCLUDED.expires_at, last_updated_at = EXCLUDED.last_updated_at")

	_, err = tx.Exec(promoQuery.String(), promoArgs...)
	if err != nil {
		return fmt.Errorf("failed to insert promos: %w", err)
	}

	return tx.Commit()
}
