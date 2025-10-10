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
	{ID: 2370, Slug: "Halloween", ParentCategoryID: 553, Percentage: 0.0006500},
	{ID: 2371, Slug: "Easter", ParentCategoryID: 553, Percentage: 0.0006200},
	{ID: 2372, Slug: "Valentines Day", ParentCategoryID: 553, Percentage: 0.0005800},
	{ID: 2373, Slug: "Thanksgiving", ParentCategoryID: 553, Percentage: 0.0005500},
	{ID: 2374, Slug: "Birthday", ParentCategoryID: 553, Percentage: 0.0005200},
	{ID: 2375, Slug: "Wedding", ParentCategoryID: 553, Percentage: 0.0004900},
	{ID: 2376, Slug: "Baby Shower", ParentCategoryID: 553, Percentage: 0.0004600},
	{ID: 2377, Slug: "Graduation", ParentCategoryID: 553, Percentage: 0.0004300},
	{ID: 2378, Slug: "Summer", ParentCategoryID: 553, Percentage: 0.0004000},
	{ID: 2379, Slug: "Spring", ParentCategoryID: 553, Percentage: 0.0003800},
	{ID: 2380, Slug: "Fall", ParentCategoryID: 553, Percentage: 0.0003600},
	{ID: 2381, Slug: "Back to School", ParentCategoryID: 553, Percentage: 0.0003400},
	{ID: 2382, Slug: "Sports", ParentCategoryID: 553, Percentage: 0.0003200},
	{ID: 2383, Slug: "Music", ParentCategoryID: 553, Percentage: 0.0003000},
	{ID: 2384, Slug: "Food & Drink", ParentCategoryID: 553, Percentage: 0.0002800},
	{ID: 2385, Slug: "Animals", ParentCategoryID: 553, Percentage: 0.0002600},
	{ID: 2386, Slug: "Nature", ParentCategoryID: 553, Percentage: 0.0002400},
	{ID: 2387, Slug: "Travel", ParentCategoryID: 553, Percentage: 0.0002200},
	{ID: 2388, Slug: "Business", ParentCategoryID: 553, Percentage: 0.0002000},
	{ID: 2389, Slug: "Education", ParentCategoryID: 553, Percentage: 0.0001800},
	{ID: 2390, Slug: "Technology", ParentCategoryID: 553, Percentage: 0.0001600},
	{ID: 637, Slug: "Paper Crafts", ParentCategoryID: 26, Percentage: 0.0001500},
	{ID: 639, Slug: "Sewing & Quilting", ParentCategoryID: 26, Percentage: 0.0001400},
	{ID: 640, Slug: "Jewelry Making", ParentCategoryID: 26, Percentage: 0.0001300},
	{ID: 641, Slug: "Scrapbooking", ParentCategoryID: 26, Percentage: 0.0001200},
	{ID: 642, Slug: "Card Making", ParentCategoryID: 26, Percentage: 0.0001100},
	{ID: 2391, Slug: "Stickers & Labels", ParentCategoryID: 553, Percentage: 0.0001000},
	{ID: 2392, Slug: "Banners & Signs", ParentCategoryID: 553, Percentage: 0.0000950},
	{ID: 2393, Slug: "Invitations", ParentCategoryID: 553, Percentage: 0.0000900},
	{ID: 2394, Slug: "Greeting Cards", ParentCategoryID: 553, Percentage: 0.0000850},
	{ID: 2395, Slug: "Planners & Journals", ParentCategoryID: 553, Percentage: 0.0000800},
	{ID: 2396, Slug: "Calendars", ParentCategoryID: 553, Percentage: 0.0000750},
	{ID: 2397, Slug: "Bookmarks", ParentCategoryID: 553, Percentage: 0.0000700},
	{ID: 2398, Slug: "Gift Tags", ParentCategoryID: 553, Percentage: 0.0000650},
	{ID: 2399, Slug: "Photo Frames", ParentCategoryID: 553, Percentage: 0.0000600},
	{ID: 2400, Slug: "Packaging", ParentCategoryID: 553, Percentage: 0.0000550},
	{ID: 2401, Slug: "Wrapping Paper", ParentCategoryID: 553, Percentage: 0.0000500},
	{ID: 11, Slug: "Handwriting", ParentCategoryID: 23, Percentage: 0.0000450},
	{ID: 15, Slug: "Slab Serif", ParentCategoryID: 23, Percentage: 0.0000400},
	{ID: 16, Slug: "Decorative", ParentCategoryID: 23, Percentage: 0.0000350},
	{ID: 737, Slug: "Machine Embroidery", ParentCategoryID: 735, Percentage: 0.0000300},
	{ID: 738, Slug: "Hand Embroidery", ParentCategoryID: 735, Percentage: 0.0000250},
	{ID: 2247, Slug: "Files", ParentCategoryID: 2245, Percentage: 0.0000200},
	{ID: 2248, Slug: "Templates", ParentCategoryID: 2245, Percentage: 0.0000150},
	{ID: 2249, Slug: "3D Models", ParentCategoryID: 2244, Percentage: 0.0000100},
	{ID: 2250, Slug: "Knitting Patterns", ParentCategoryID: 2246, Percentage: 0.0000050},
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

	// Validate count for modes that require it
	if (*mode == "subcategories" || *mode == "products" || *mode == "promos" || *mode == "downloads") && *count <= 0 {
		log.Fatal("Error: -count flag is required and must be > 0 for 'subcategories', 'products', 'promos', and 'downloads' modes")
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
		if err := importSubcategories(db, *count); err != nil {
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

func importSubcategories(db *sql.DB, subcategoryCount int) error {
	fmt.Println("\n=== Importing Subcategories ===\n")
	fmt.Printf("Importing %d subcategories...\n", subcategoryCount)

	// Get all categories to randomly assign as parents
	rows, err := db.Query("SELECT category_id FROM category WHERE parent_category_id IS NULL")
	if err != nil {
		return fmt.Errorf("failed to fetch categories: %w", err)
	}
	defer rows.Close()

	var parentCategories []int64
	for rows.Next() {
		var catID int64
		if err := rows.Scan(&catID); err != nil {
			return fmt.Errorf("failed to scan category: %w", err)
		}
		parentCategories = append(parentCategories, catID)
	}
	rows.Close()

	if len(parentCategories) == 0 {
		return fmt.Errorf("no parent categories found - please import categories first")
	}

	fmt.Printf("Found %d parent categories\n", len(parentCategories))

	// Get starting subcategory ID
	var startID int64
	err = db.QueryRow("SELECT COALESCE(MAX(category_id), 10000) FROM category WHERE parent_category_id IS NOT NULL").Scan(&startID)
	if err != nil {
		return fmt.Errorf("failed to get starting subcategory ID: %w", err)
	}
	startID++ // Start from next available ID

	bar := progressbar.NewOptions(subcategoryCount,
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
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate subcategory names
	subcategoryPrefixes := []string{"Modern", "Vintage", "Classic", "Premium", "Professional", "Creative", "Elegant", "Bold", "Minimal", "Decorative"}
	subcategorySuffixes := []string{"Designs", "Templates", "Graphics", "Elements", "Patterns", "Styles", "Collections", "Sets", "Packs", "Kits"}

	for i := 0; i < subcategoryCount; i++ {
		subcatID := startID + int64(i)
		parentCatID := parentCategories[rng.Intn(len(parentCategories))]
		
		// Generate random subcategory name
		prefix := subcategoryPrefixes[rng.Intn(len(subcategoryPrefixes))]
		suffix := subcategorySuffixes[rng.Intn(len(subcategorySuffixes))]
		slug := fmt.Sprintf("%s-%s-%d", strings.ToLower(prefix), strings.ToLower(suffix), subcatID)

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
			ON CONFLICT (category_id) DO NOTHING
		`, subcatID, parentCatID, slug, fmt.Sprintf("Description for %s", slug), slug, time.Now(), time.Now())

		if err != nil {
			return fmt.Errorf("failed to insert subcategory %d: %w", subcatID, err)
		}
		inserted++
		bar.Add(1)
	}

	fmt.Printf("\n  ✓ Inserted: %d subcategories\n\n", inserted)
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

	// Load subcategories from database
	fmt.Println("Loading subcategories from database...")
	rows, err := db.Query("SELECT category_id, parent_category_id FROM category WHERE parent_category_id IS NOT NULL")
	if err != nil {
		return fmt.Errorf("failed to load subcategories: %w", err)
	}
	defer rows.Close()

	subcategoryList := make(map[int64][]int64) // parent_category_id -> []subcategory_ids
	var totalSubcategories int
	for rows.Next() {
		var subcatID, parentCatID int64
		if err := rows.Scan(&subcatID, &parentCatID); err != nil {
			return fmt.Errorf("failed to scan subcategory: %w", err)
		}
		subcategoryList[parentCatID] = append(subcategoryList[parentCatID], subcatID)
		totalSubcategories++
	}
	rows.Close()

	if totalSubcategories == 0 {
		return fmt.Errorf("no subcategories found - please import subcategories first")
	}

	fmt.Printf("Loaded %d subcategories\n", totalSubcategories)

	// Build weighted category distribution
	categoryWeights := buildCategoryWeights()

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
				if err := insertProductBatch(db, batch.startID, batch.count, categoryWeights, subcategoryList, rng); err != nil {
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

func selectCategoryByWeight(rng *rand.Rand, weights []float64) int64 {
	r := rng.Float64()
	for i, w := range weights {
		if r <= w {
			return categories[i].ID
		}
	}
	return categories[len(categories)-1].ID
}

func selectSubcategories(rng *rand.Rand, categoryID int64, subcategoryList map[int64][]int64) []int64 {
	subcats, exists := subcategoryList[categoryID]
	if !exists || len(subcats) == 0 {
		return []int64{}
	}

	// Select 1-3 random subcategories for this category
	numSubs := rng.Intn(3) + 1
	if numSubs > len(subcats) {
		numSubs = len(subcats)
	}

	// Shuffle and pick first numSubs
	shuffled := make([]int64, len(subcats))
	copy(shuffled, subcats)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled[:numSubs]
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

func insertProductBatch(db *sql.DB, startID int64, count int, categoryWeights []float64, subcategoryList map[int64][]int64, rng *rand.Rand) error {
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
		subcategoryIDs := selectSubcategories(rng, categoryID, subcategoryList)
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

func importDownloads(db *sql.DB, downloadCount int) error {
	fmt.Println("\n=== Importing Product Downloads ===\n")
	fmt.Printf("Importing %d downloads using %d workers...\n", downloadCount, numWorkers)

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

	// Pre-generate timestamps with hour precision for last 2 weeks
	fmt.Println("Pre-generating timestamps...")
	timestamps := generateHourlyTimestamps(14) // 14 days = 2 weeks
	fmt.Printf("Generated %d unique hourly timestamps\n", len(timestamps))

	// Get the starting download ID
	var startDownloadID int64
	err = db.QueryRow("SELECT COALESCE(MAX(download_id), 0) FROM product_download").Scan(&startDownloadID)
	if err != nil {
		return fmt.Errorf("failed to get starting download ID: %w", err)
	}
	startDownloadID++ // Start from next available ID

	bar := progressbar.NewOptions(downloadCount,
		progressbar.OptionSetDescription("Downloads"),
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
	type downloadBatch struct {
		startDownloadID int64
		count           int
	}

	jobs := make(chan downloadBatch, 100)
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
				if err := insertDownloadBatch(db, batch.startDownloadID, batch.count, totalProducts, timestamps, rng); err != nil {
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
		currentDownloadID := startDownloadID
		remaining := downloadCount
		batchSize := 5000 // 5000 downloads per batch

		for remaining > 0 {
			count := batchSize
			if remaining < batchSize {
				count = remaining
			}

			jobs <- downloadBatch{startDownloadID: currentDownloadID, count: count}
			currentDownloadID += int64(count)
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

	fmt.Printf("\n  ✓ Inserted: %d downloads\n\n", totalInserted)
	return nil
}

func generateHourlyTimestamps(days int) []time.Time {
	timestamps := make([]time.Time, 0, days*24)
	now := time.Now()

	// Truncate to hour precision
	currentHour := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location())

	// Generate timestamps for the last N days, one per hour
	for i := 0; i < days*24; i++ {
		timestamp := currentHour.Add(-time.Duration(i) * time.Hour)
		timestamps = append(timestamps, timestamp)
	}

	return timestamps
}

func insertDownloadBatch(db *sql.DB, startDownloadID int64, count int, totalProducts int64, timestamps []time.Time, rng *rand.Rand) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build batch insert for downloads
	var downloadQuery strings.Builder
	downloadQuery.WriteString("INSERT INTO product_download (download_id, product_id, downloaded_at, downloaded_at_day_normalized) VALUES ")

	downloadArgs := make([]interface{}, 0, count*4)

	for i := 0; i < count; i++ {
		if i > 0 {
			downloadQuery.WriteString(", ")
		}

		downloadID := startDownloadID + int64(i)

		// Random product ID (1 to totalProducts)
		productID := rng.Int63n(totalProducts) + 1

		// Pick random timestamp from pre-generated list
		downloadedAt := timestamps[rng.Intn(len(timestamps))]

		// Calculate day normalized (Unix timestamp / 86400)
		dayNormalized := downloadedAt.Unix() / 86400

		argPos := i*4 + 1
		downloadQuery.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d)",
			argPos, argPos+1, argPos+2, argPos+3))

		downloadArgs = append(downloadArgs,
			downloadID,
			productID,
			downloadedAt,
			dayNormalized,
		)
	}

	_, err = tx.Exec(downloadQuery.String(), downloadArgs...)
	if err != nil {
		return fmt.Errorf("failed to insert downloads: %w", err)
	}

	return tx.Commit()
}
