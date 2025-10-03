
Supermarket publisher
========================

This repository defines cron jobs that run every day to scrape Israeli supermarket data, process it, and publish it to long term and short term data storage

The code is used from:
- Fetch the data from all supermarkets (every X hours)
- Convert to data dataframe.
- Compose Kaggle dataset with a binding between the version number and the scrape date (at midnight)
- Create new version in Kaggle (at midnight)

[Link to Kaggle Dataset](https://www.kaggle.com/datasets/erlichsefi/israeli-supermarkets-2024)

[![Last Publish Data to Kaggle Status](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/_prod_publishing.yml/badge.svg)](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/actions/workflows/_prod_publishing.yml)

---

## Docker Build Targets

The Dockerfile provides four different build targets for different use cases:

### 1. **dev** (Development)
Development environment with all dependencies including dev tools.

```bash
docker build --target dev -t supermarket-dev .
```

### 2. **testing** (Unit Tests & System Tests)
Runs system tests and pytest suite for continuous integration.

```bash
docker build --target testing -t supermarket-testing .
docker run supermarket-testing
```

This target executes:
- System validation tests (`python system_tests/main.py`)
- All unit tests (`python -m pytest .`)

### 3. **data_processing** (Data Scraping & Processing)
The main workhorse that scrapes supermarket data, processes it, and publishes to databases.

**What it does:**
- Scrapes data from Israeli supermarket chains
- Converts raw XML/HTML to structured format
- Stores data in short-term database (MongoDB/Kafka)
- Publishes to long-term storage (Kaggle)
- Monitors health via heartbeat mechanism

See the [Data Processing Docker Example](#data-processing-example) below for usage.

### 4. **serving** (API Server)
FastAPI server that provides REST API access to the scraped raw data.

**Endpoints:**
- `GET /list_chains` - List all available supermarket chains
- `GET /list_file_types` - List all available file types (Prices, Promos, Stores)
- `GET /list_scraped_files` - List scraped files with filters
- `GET /raw/file_content` - Get raw file content with pagination
- `GET /service_health` - API service health check
- `GET /short_term_health` - MongoDB health status
- `GET /long_term_health` - Kaggle dataset health status

See the [API Docker Example](#api-example) below for usage.

---

## Health Check System

### `healthcheck.py`

The health check script validates that the data processing service is running correctly. It's used by Docker's `HEALTHCHECK` directive to monitor container health.

**What it checks:**
1. **Heartbeat file exists and is recent** - Ensures the process is actively running
2. **No operations have failed** - Validates that scraping/processing operations are successful
3. **Timestamp is within acceptable range** - Detects if the process has stalled

**Exit codes:**
- `0` - Service is healthy
- `1` - Service is unhealthy (triggers container restart if configured)

**Configuration:**
- `APP_DATA_PATH` - Directory containing the heartbeat file (default: `app_data`)
- `HEALTHCHECK_MAX_AGE_SECONDS` - Maximum age for heartbeat before considering unhealthy (default: `300` seconds = 5 minutes)

The health check integrates with the `HeartbeatManager` utility which the data processor updates during operations.

---

## Docker Run Examples

### <a name="data-processing-example"></a>Data Processing Example

```bash
docker run -d \
  --name supermarket-data-processor \
  -e MONGODB_URI="mongodb://user:password@mongodb:27017/" \  # MongoDB connection string for short-term storage
  -e KAGGLE_KEY="your_kaggle_key" \  # Kaggle API key from ~/.kaggle/kaggle.json
  -e KAGGLE_USERNAME="your_username" \  # Your Kaggle username
  -e KAGGLE_DATASET_REMOTE_NAME="username/dataset-name" \  # Target Kaggle dataset (format: username/dataset-name)
  -e APP_DATA_PATH="/app/app_data" \  # Directory for application data, logs, and heartbeat files
  -e OUTPUT_DESTINATION="mongo" \  # Short-term storage destination (options: mongo, kafka, file)
  -e NUM_OF_PROCESSES="5" \  # Number of parallel scraping/processing processes
  -e ENABLED_SCRAPERS="VICTORY,COFIX" \  # Comma-separated list of scrapers to enable from il_supermarket_scarper.ScraperFactory (empty = all)
  -e ENABLED_FILE_TYPES="PROMO_FILE,STORE_FILE,PRICE_FILE,PROMO_FULL_FILE,PRICE_FULL_FILE" \  # sublist of il_supermarket_scarper.FileTypesFilters
  -e LIMIT="100" \  # Optional: Limit number of files to process (useful for testing)
  -e WAIT_TIME_SECONDS="1800" \  # Seconds to wait between operation cycles (default: 1800 = 30 minutes)
  -e STOP="EOD" \  # When to execute final operations (options: EOD = End of Day, NEVER)
  -e REPEAT="FOREVER" \  # Whether to repeat the DAG (options: FOREVER, ONCE)
  -e OPERATION="" \  # Optional: Execute specific operations only (comma-separated: scraping,converting,api_update,publishing,clean_dump_files,clean_all_source_data, if not spesificed execute scraping,converting,api_update,clean_dump_files at Stop on publishing,clean_all_source_data
  -e WHEN="" \  # Optional: Override timestamp for data processing (ISO format: 2024-01-01T00:00:00)
  -e LOG_LEVEL="INFO" \  # Logging level (options: DEBUG, INFO, WARNING, ERROR)
  -e HEALTHCHECK_MAX_AGE_SECONDS="300" \  # Maximum seconds before healthcheck fails (default: 300)
  -v /path/to/app_data:/app/app_data \  # Mount volume for persistent data
  supermarket-data-processor
```

**Common use cases:**

**Run once and exit (for testing):**
```bash
docker run --rm \
  -e REPEAT="ONCE" \
  -e LIMIT="10" \
  -e ENABLED_SCRAPERS="shufersal" \
  # ... other env vars ...
  supermarket-data-processor
```

**Execute specific operation only:**
```bash
docker run --rm \
  -e OPERATION="scraping" \  # Only run the scraping operation
  # ... other env vars ...
  supermarket-data-processor
```

---

### <a name="api-example"></a>API Server Example

```bash
docker run -d \
  --name supermarket-api \
  -p 8000:8000 \  # Expose API on port 8000
  -e MONGODB_URI="mongodb://user:password@mongodb:27017/" \  # MongoDB connection string where scraped data is stored
  -e KAGGLE_KEY="your_kaggle_key" \  # Kaggle API key for accessing published datasets
  -e KAGGLE_USERNAME="your_username" \  # Your Kaggle username
  -e KAGGLE_DATASET_REMOTE_NAME="username/dataset-name" \  # Kaggle dataset to serve (format: username/dataset-name)
  -e SUPABASE_URL="https://your-project.supabase.co" \  # Supabase URL for authentication
  -e SUPABASE_KEY="your_supabase_key" \  # Supabase anonymous key for token validation
  -e LOG_LEVEL="WARNING" \  # Logging level (options: DEBUG, INFO, WARNING, ERROR)
  supermarket-api
```

**Access the API:**

After starting the container, the API will be available at:
- **API Base:** `http://localhost:8000`
- **Interactive Docs:** `http://localhost:8000/docs` (Swagger UI)
- **OpenAPI Schema:** `http://localhost:8000/openapi.json`

**Example API calls:**

```bash
# Get authentication token (from Supabase)
TOKEN="your_jwt_token"

# List all available chains
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/list_chains

# List scraped files for a specific chain
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/list_scraped_files?chain=shufersal&only_latest=true"

# Get file content
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/raw/file_content?chain=shufersal&file=PriceFull7290027600007-001-202410010000.xml"

# Check service health
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/service_health
```

---

## Development Setup


Testing:
```bash
./run_test.sh
```

---

## Environment Variables Reference

### Required for Data Processing
- `MONGODB_URI` - MongoDB connection string
- `KAGGLE_KEY` - Kaggle API key
- `KAGGLE_USERNAME` - Kaggle username
- `KAGGLE_DATASET_REMOTE_NAME` - Target Kaggle dataset
- `APP_DATA_PATH` - Application data directory

### Required for API
- `MONGODB_URI` - MongoDB connection string
- `KAGGLE_KEY` - Kaggle API key
- `KAGGLE_USERNAME` - Kaggle username
- `KAGGLE_DATASET_REMOTE_NAME` - Kaggle dataset to serve
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_KEY` - Supabase anonymous key

### Optional Configuration
- `NUM_OF_PROCESSES` - Parallel processes (default: 5)
- `ENABLED_SCRAPERS` - Filter specific scrapers (comma-separated)
- `ENABLED_FILE_TYPES` - Filter file types (comma-separated)
- `OUTPUT_DESTINATION` - Storage target: mongo/kafka/file (default: mongo)
- `LOG_LEVEL` - Logging level (default: WARNING)
- `LIMIT` - Limit number of files to process
- `WAIT_TIME_SECONDS` - Seconds between cycles (default: 1800)
- `HEALTHCHECK_MAX_AGE_SECONDS` - Health check timeout (default: 300)


---

## License

See [LICENSE.txt](LICENSE.txt) for details.
