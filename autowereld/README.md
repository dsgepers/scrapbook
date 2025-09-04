# Autowereld Batch Planning System

This system fetches brand and model data from autowereld.nl and organizes it into optimized groups for batch processing, storing the results in a SQLite database.

## Files

1. **`init_database.py`** - Initializes the SQLite database with the required table structure
2. **`fetch_brands.py`** - Fetches brand and model data and creates optimized batch groups
3. **`scrape_listings.py`** - Main production scraper that processes all batch planning records
4. **`reset_database.py`** - Reset batch processing status and clear results table
5. **`result.db`** - SQLite database containing batch planning and scraped results

### Test/Development Files:
- **`test_scraper.py`** - Test single page scraping functionality
- **`test_batch_scraper.py`** - Test batch scraping with limited pages
- **`scrape_single_batch.py`** - Process one batch from batch planning table

## Usage

1. **Initialize the database:**
   ```bash
   python3 init_database.py
   ```

2. **Fetch and group brand/model data:**
   ```bash
   python3 fetch_brands.py
   ```

3. **Scrape vehicle listings:**
   ```bash
   python3 scrape_listings.py
   ```

4. **Reset database (if needed):**
   ```bash
   python3 reset_database.py
   ```

### Development/Testing

- **Test single page scraping:**
  ```bash
  python3 test_scraper.py
  ```

- **Test batch scraping (limited pages):**
  ```bash
  python3 test_batch_scraper.py
  ```

- **Process single batch:**
  ```bash
  python3 scrape_single_batch.py
  ```

## Database Schema

The database contains two tables:

### `autowereld_batch_planning` table:
- `id` - Primary key
- `brand_keys` - Pipe-separated list of brand identifiers
- `models_keys` - Pipe-separated list of model identifiers (for large brands)
- `results_expected` - Total number of search results expected
- `results_found` - Placeholder for actual results found (set to 0 initially)

### `autowereld_results` table:
- `id` - Primary key (auto-increment)
- `identifier` - Unique identifier for the listing (required)
- `url` - URL to the listing (required)
- `licenseplate` - Vehicle license plate (nullable)
- `construction_year` - Year the vehicle was built (nullable)
- `mileage` - Vehicle mileage in kilometers (nullable)
- `price` - Listing price in cents/euros (nullable)
- `seller_name` - Name of the seller (nullable)
- `seller_identifier` - Unique identifier for the seller (nullable)
- `tags` - Pipe-separated list of tags/features (nullable)

## How It Works

### Phase 1: Brand Analysis (`fetch_brands.py`)
1. **Brand Data Fetching**: Makes HTTP requests to autowereld.nl to get all available brands and their result counts
2. **Grouping Algorithm**: Groups brands to stay within the 9000 result limit using an ascending sort + greedy bin packing approach
3. **Large Brand Processing**: For brands exceeding 9000 results, fetches their individual models and groups those instead
4. **Database Storage**: Saves all grouped results to SQLite for batch processing

### Phase 2: Listing Scraping (`scrape_listings.py`)
1. **Batch Processing**: Loops through each batch planning record
2. **Page Scraping**: Fetches search result pages with 100 listings each
3. **Data Extraction**: Parses each `article.item` element to extract:
   - Identifier and URL
   - Mileage and construction year
   - Price information
   - Seller details
   - Vehicle specifications/tags
4. **Pagination**: Follows "next" page links until no more pages exist
5. **Database Storage**: Saves each unique listing to the `autowereld_results` table
6. **Progress Tracking**: Updates `results_found` for each completed batch

## Results Summary

- **Total Groups**: 48 optimized batch groups
- **Total Expected Results**: 326,726 search results across all groups
- **Largest Groups**: Around 8,800-8,900 results each (well under the 9000 limit)
- **Large Brands Processed**: audi, toyota, opel, bmw, peugeot, ford, renault, mercedes-benz, volkswagen

## Key Features

- **European Number Format Handling**: Correctly parses numbers like "15.211" as 15,211
- **Smart Grouping**: Optimizes group sizes to maximize efficiency while staying under limits
- **Model-Level Grouping**: Automatically handles large brands by grouping their models instead
- **Database Integration**: Stores results in structured format ready for analysis
- **Duplicate Prevention**: Prevents saving duplicate listings using identifier checking
- **Robust Pagination**: Correctly follows next page links until completion
- **Error Handling**: Comprehensive error handling for HTTP requests and data parsing
- **Progress Tracking**: Updates batch completion status in real-time
- **Respectful Scraping**: Includes delays between requests to be respectful to the server
