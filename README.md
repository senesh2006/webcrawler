# Python Pipeline Web Crawler

This is a powerful, multi-stage pipeline web crawler built in Python. It is designed to be flexible and robust, capable of scraping both static websites (like blogs) and modern, dynamic applications that rely heavily on JavaScript.

All extracted data (pages, links, images, and text content) is saved into a structured SQLite database (`crawler.db`) for easy analysis.

This project features:

- **Hybrid Fetching**: Can use the fast `requests` library for simple HTML pages or switch to a full Playwright (headless browser) to render JavaScript-heavy sites.

- **Multi-Threaded Pipeline**: Uses a multi-stage, multi-worker architecture to process URLs in parallel, handling fetching, parsing, and storage efficiently.

- **Rich Data Extraction**: Scrapes and saves full HTML, clean text content, page metadata, all links (with anchor text), and image URLs.

- **Highly Configurable**: All pipeline settings, from worker counts to headless mode, are managed via YAML configuration files.

- **Polite & Robust**: Automatically respects `robots.txt` rules, manages domain-specific rate-limiting, and handles duplicate URL detection.

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/Senes2004/Webcrawler.git
cd Webcrawler
```

2. **Install Python dependencies:**
   It's recommended to use a virtual environment.
```bash
pip install -r requirements.txt
```

   (If you don't have a `requirements.txt` file, run: `pip install requests playwright pyyaml beautifulsoup4 urllib3`)

3. **Install Playwright Browsers:**
   This is a mandatory step. It downloads the headless browsers (like Chromium) that Playwright needs to run.
```bash
playwright install
```

## How to Use

All commands should be run from the `src/` directory:
```bash
cd src
```

### Example 1: Basic Crawl (Static Site)

This will use the default configuration (`config.yaml`), which is set up to be fast (using `requests`) and polite.

**File:** `config.yaml`

- **Mode:** `use_headless_browser: false` (Fast requests mode)
- **Workers:** `storage_workers: 1` (Safe for SQLite)
```bash
# Crawl quotes.toscrape.com with a max depth of 3
python3 -m web_crawler.cli crawl https://quotes.toscrape.com -d 3
```

### Example 2: Advanced Crawl (JavaScript-Heavy Site)

To crawl a site like `finance.yahoo.com`, you must use the headless browser. We do this by creating an override config file.

**1. Create `yahoo.yaml` in the `src/` directory:**
   This file overrides the defaults.
```yaml
# This is yahoo.yaml
pipeline:
  storage_workers: 1  # Ensures no database locks

stages:
  url_validation:
    # Stay on this domain
    allowed_domains:
      - finance.yahoo.com

  fetch:
    # The most important part:
    use_headless_browser: true
    page_load_delay: 2.0  # Give JS 2 seconds to load
  
  link_reinjection:
    # Override the 5-page limit bug from the CLI
    max_total_pages: 100
```

**2. Run the crawl using the `-c` flag:**
```bash
# -c tells the crawler to use your yahoo.yaml file
# -d 1 sets max depth to 1
python3 -m web_crawler.cli crawl -c yahoo.yaml https://finance.yahoo.com -d 1
```

## Project Structure

- `database.c`: A simple C program to read the output database.

- `src/data/crawler.db`: The output SQLite database where all data is stored.

- `src/logs/crawler.log`: The main log file for debugging.

- `src/web_crawler/`: The main Python source code.
  - `cli.py`: The command-line interface entry point.
  - `config/`: Manages loading `config.yaml` files.
  - `core/`: Contains the main `PipelineCrawler` logic.
  - `pipeline/`: Contains the base classes for the pipeline.
  - `pipeline/stages/`: Contains every individual stage of the crawl (fetch, parse, store, etc.).

## Viewing the Data

All your data is saved in `src/data/crawler.db`. You can view it in two ways:

### 1. Using the SQLite3 Terminal
```bash
# Navigate to the data directory
cd src/data

# Open the database file
sqlite3 crawler.db
```

Once inside `sqlite>`, run these commands:
```sql
.tables
.headers on
.mode column

SELECT url, word_count, status_code FROM pages LIMIT 10;
SELECT source_url, target_url, anchor_text FROM links LIMIT 10;
```

### 2. Using the C Program

A simple C program (`database.c`) is included to show how to read the `pages` table.
```bash
# From the Webcrawler/ folder, compile the C file:
# (On Linux/macOS)
gcc database.c -o analyze -lsqlite3

# (On Windows with MinGW/GCC)
gcc database.c -o analyze.exe -lsqlite3

# Run the compiled program
./analyze
```
