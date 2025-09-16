#!/usr/bin/env python3
"""
Production scraper - process one batch from autowereld_batch_planning table.
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import random
import math
import concurrent.futures
from urllib.parse import urljoin, urlparse


def build_proxy_url(base_proxy_url, href):
    """
    Build a proper proxy URL preserving the /fireprox/ path and using zoeken.html endpoint.
    
    Args:
        base_proxy_url: The proxy base URL (e.g., 'https://rez5adtep6.execute-api.eu-central-1.amazonaws.com/fireprox/')
        href: The href from the next link (could be relative or absolute)
    
    Returns:
        Properly constructed proxy URL using zoeken.html endpoint
    """
    if not href:
        return None
        
    # Ensure base_proxy_url ends with /fireprox/
    if not base_proxy_url.endswith('/fireprox/'):
        if base_proxy_url.endswith('/fireprox'):
            base_proxy_url += '/'
        elif base_proxy_url.endswith('/'):
            base_proxy_url += 'fireprox/'
        else:
            base_proxy_url += '/fireprox/'
    
    # If href starts with /, it's an absolute path - extract query parameters and use zoeken.html
    if href.startswith('/'):
        # Find query parameters (everything after ?)
        if '?' in href:
            query_params = href.split('?', 1)[1]
            return base_proxy_url + 'zoeken.html?' + query_params
        else:
            # No query params, just use zoeken.html
            return base_proxy_url + 'zoeken.html'
    
    # If href is relative (like ?p=2), use urljoin as normal
    return urljoin(base_proxy_url, href)


def scrape_single_page(url, headers, page_num):
    """
    Scrape a single page and return the results.
    
    Args:
        url: The URL to scrape
        headers: Request headers
        page_num: Page number for logging
        
    Returns:
        tuple: (success, listings_data, total_found, page_num)
    """
    try:
        print(f"  Scraping page {page_num}: {url[:80]}...")
        response = requests.get(url, headers=headers, timeout=30)
        
        # Handle 404 as expected end of pagination
        if response.status_code == 404:
            print(f"  Page {page_num}: 404 - End of results")
            return (False, [], 0, page_num)
            
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('article', class_='item')
        
        if len(articles) == 0:
            print(f"  Page {page_num}: No articles found")
            return (False, [], 0, page_num)
        
        # Parse all listings on this page
        listings_data = []
        for article in articles:
            listing_data = parse_listing(article, url)
            if listing_data:
                listings_data.append(listing_data)
        
        print(f"  Page {page_num}: Found {len(articles)} listings, parsed {len(listings_data)}")
        return (True, listings_data, len(articles), page_num)
        
    except requests.exceptions.RequestException as e:
        print(f"  Page {page_num}: Request error - {e}")
        return (False, [], 0, page_num)
    except Exception as e:
        print(f"  Page {page_num}: Parse error - {e}")
        return (False, [], 0, page_num)


def process_parallel_pages(base_url, headers, start_page, end_page):
    """
    Process multiple pages in parallel.
    
    Args:
        base_url: Base URL template
        headers: Request headers
        start_page: Starting page number
        end_page: Ending page number (inclusive)
        
    Returns:
        tuple: (all_listings_data, total_processed, last_successful_page)
    """
    all_listings = []
    total_processed = 0
    last_successful_page = start_page - 1
    
    # Build URLs for all pages
    page_urls = []
    for page_num in range(start_page, end_page + 1):
        if page_num == 1:
            # First page doesn't need &p= parameter
            url = base_url
        else:
            # Add page parameter
            separator = '&' if '?' in base_url else '?'
            url = f"{base_url}{separator}p={page_num}"
        page_urls.append((url, page_num))
    
    print(f"Processing pages {start_page}-{end_page} in parallel...")
    
    # Process pages in parallel with limited workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all page requests
        future_to_page = {
            executor.submit(scrape_single_page, url, headers, page_num): page_num 
            for url, page_num in page_urls
        }
        
        # Collect results as they complete
        results = []
        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                success, listings_data, found_count, returned_page = future.result()
                results.append((returned_page, success, listings_data, found_count))
            except Exception as e:
                print(f"  Page {page_num}: Exception - {e}")
                results.append((page_num, False, [], 0))
    
    # Sort results by page number and process
    results.sort(key=lambda x: x[0])
    
    for page_num, success, listings_data, found_count in results:
        if success:
            all_listings.extend(listings_data)
            total_processed += found_count
            last_successful_page = page_num
        else:
            # If we hit a 404 or error, note it but continue with other pages
            if found_count == 0:  # This was a 404 or empty page
                print(f"  Page {page_num}: No more results")
    
    return all_listings, total_processed, last_successful_page


def get_random_googlebot_ip():
    """Generate a random IP from Googlebot IP ranges."""
    # Define the three IP ranges: 66.249.79.96/27, 66.249.79.64/27, 66.249.79.32/27
    ranges = [
        (0x42F94F60, 0x42F94F7F),  # 66.249.79.96/27 (96-127)
        (0x42F94F40, 0x42F94F5F),  # 66.249.79.64/27 (64-95)
        (0x42F94F20, 0x42F94F3F),  # 66.249.79.32/27 (32-63)
    ]
    
    # Choose a random range
    start, end = random.choice(ranges)
    
    # Generate a random IP within the chosen range
    ip_int = random.randint(start, end)
    
    # Convert to IP string
    return f"{(ip_int >> 24) & 0xFF}.{(ip_int >> 16) & 0xFF}.{(ip_int >> 8) & 0xFF}.{ip_int & 0xFF}"


def extract_number(text):
    """Extract number from text, handling European formatting."""
    if not text:
        return None
    
    # Remove non-digit characters except dots and spaces
    cleaned = re.sub(r'[^\d\.\s]', '', text)
    # Remove dots (thousands separators) and spaces
    cleaned = cleaned.replace('.', '').replace(' ', '')
    
    try:
        return int(cleaned) if cleaned else None
    except ValueError:
        return None


def extract_year_month(date_text):
    """Extract year from date text like '11-2020'."""
    if not date_text:
        return None
    
    # Look for pattern MM-YYYY or YYYY
    match = re.search(r'(\d{1,2}-)(\d{4})|(\d{4})', date_text)
    if match:
        if match.group(2):  # MM-YYYY format
            return int(match.group(2))
        elif match.group(3):  # YYYY format
            return int(match.group(3))
    
    return None


def parse_listing(article, base_url):
    """Parse a single article.item element into listing data."""
    try:
        # Extract URL and identifier
        link_elem = article.find('a', class_='frame')
        if not link_elem:
            return None
            
        relative_url = link_elem.get('href', '')
        full_url = urljoin(base_url, relative_url)
        
        # Extract identifier from data-nr attribute or URL
        identifier = None
        action_elem = article.find('div', {'data-nr': True})
        if action_elem:
            identifier = action_elem.get('data-nr')
        
        if not identifier:
            # Try to extract from URL
            url_match = re.search(r'-(\d+)/', relative_url)
            if url_match:
                identifier = url_match.group(1)
        
        if not identifier:
            print(f"Warning: Could not extract identifier from {relative_url}")
            return None
        
        # Extract seller information
        seller_elem = article.find('div', class_='seller')
        seller_name = None
        if seller_elem:
            name_elem = seller_elem.find('span', class_='name')
            if name_elem:
                seller_name = name_elem.get_text(strip=True)
        
        # Extract mileage and construction year
        mileage_build_elem = article.find('span', class_='text-mileage-build')
        mileage = None
        construction_year = None
        
        if mileage_build_elem:
            text = mileage_build_elem.get_text(strip=True)
            # Handle both " - " and "- " patterns
            parts = re.split(r'\s*-\s*', text, 1)
            
            if len(parts) >= 1:
                mileage = extract_number(parts[0])
            
            if len(parts) >= 2:
                construction_year = extract_year_month(parts[1])
        
        # Try alternative locations for mileage and build year
        if not mileage:
            mileage_elem = article.find('div', class_='mileage')
            if mileage_elem:
                mileage = extract_number(mileage_elem.get_text(strip=True))
        
        if not construction_year:
            build_elem = article.find('div', class_='build')
            if build_elem:
                construction_year = extract_year_month(build_elem.get_text(strip=True))
        
        # Extract price
        price = None
        price_elem = article.find('div', class_='price')
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            price = extract_number(price_text)
        
        # Extract tags from specs
        tags = []
        specs_elem = article.find('ul', class_='specs')
        if specs_elem:
            for li in specs_elem.find_all('li'):
                tag_text = li.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)
        
        # Add energy label if present
        energy_elem = article.find('label', class_='energylabel')
        if energy_elem:
            energy_text = energy_elem.find('span', class_='text')
            if energy_text:
                energy_label = energy_text.get_text(strip=True)
                if energy_label:
                    tags.append(f"Energielabel {energy_label}")
        
        tags_string = '|'.join(tags) if tags else None
        
        return (
            identifier,           # identifier
            full_url,            # url
            None,                # licenseplate (not in listing overview)
            construction_year,   # construction_year
            mileage,            # mileage
            price,              # price
            seller_name,        # seller_name
            None,               # seller_identifier (not available in overview)
            tags_string         # tags
        )
        
    except Exception as e:
        print(f"Error parsing listing: {e}")
        return None


def save_listing_to_db(listing_data):
    """Save a single listing to the autowereld_results table."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if listing already exists
    cursor.execute("SELECT id FROM autowereld_results WHERE identifier = ?", (listing_data[0],))
    if cursor.fetchone():
        conn.close()
        return False  # Already exists
    
    cursor.execute("""
        INSERT INTO autowereld_results 
        (identifier, url, licenseplate, construction_year, mileage, price, 
         seller_name, seller_identifier, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, listing_data)
    
    conn.commit()
    conn.close()
    return True


def update_results_found(batch_id, count):
    """Update the results_found field for a batch planning record."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE autowereld_batch_planning 
        SET results_found = ? 
        WHERE id = ?
    """, (count, batch_id))
    
    conn.commit()
    conn.close()


def scrape_single_batch_by_id(batch_id):
    """Scrape a specific batch by its ID."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    # Get the specific batch
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, brand_keys, models_keys, results_expected 
        FROM autowereld_batch_planning 
        WHERE id = ?
    """, (batch_id,))
    
    record = cursor.fetchone()
    conn.close()
    
    if not record:
        print(f"Batch {batch_id} not found!")
        return 0
    
    batch_id, brand_keys, models_keys, expected_results = record
    
    print(f"Processing batch {batch_id}: {brand_keys}")
    print(f"Models: {models_keys[:100]}..." if models_keys and len(models_keys) > 100 else f"Models: {models_keys}")
    print(f"Expected results: {expected_results}")
    
    # Build the URL
    base_url = "https://rez5adtep6.execute-api.eu-central-1.amazonaws.com/fireprox/zoeken.html"
    params = ['il=100']  # 100 items per page
    
    if brand_keys:
        params.append(f'mrk={brand_keys}')
    
    if models_keys:
        params.append(f'mdl={models_keys}')
    
    url = f"{base_url}?{'&'.join(params)}"
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'referer': 'https://www.autowereld.nl/',
        'user-agent': 'Googlebot',
        #'X-My-X-Forwarded-For': get_random_googlebot_ip()
    }
    
    # PREDICTIVE PAGINATION STRATEGY
    # Calculate predicted number of pages based on expected results
    predicted_pages = math.ceil(expected_results / 100)
    print(f"Predicted pages based on expected results: {predicted_pages}")
    
    total_listings = 0
    new_listings = 0
    all_listings_data = []
    
    # Process predicted pages in parallel batches of 10
    current_page = 1
    
    while current_page <= predicted_pages:
        batch_end = min(current_page + 9, predicted_pages)  # Process 10 pages at a time
        
        print(f"\nProcessing predicted pages {current_page}-{batch_end} ({batch_end - current_page + 1} pages)...")
        
        # Process this batch of pages in parallel
        batch_listings, batch_processed, last_successful = process_parallel_pages(
            url, headers, current_page, batch_end
        )
        
        # Save all listings from this batch
        batch_new = 0
        for listing_data in batch_listings:
            try:
                if save_listing_to_db(listing_data):
                    batch_new += 1
                    new_listings += 1
                total_listings += 1
            except Exception as e:
                print(f"  Error saving listing {listing_data[0] if listing_data else 'unknown'}: {e}")
        
        print(f"Batch {current_page}-{batch_end}: Processed {batch_processed} listings, saved {batch_new} new")
        
        # If we didn't get results from the last page in this batch, we've likely hit the end
        if last_successful < batch_end:
            print(f"Hit end of results at page {last_successful}, stopping predictive phase")
            break
            
        current_page = batch_end + 1
        
        # Small delay between parallel batches
        if current_page <= predicted_pages:
            time.sleep(0.5)
    
    # SEQUENTIAL PAGINATION FALLBACK
    # If we processed all predicted pages successfully, continue with sequential pagination
    if current_page > predicted_pages:
        print(f"\nPredictive phase complete. Checking for additional pages beyond {predicted_pages}...")
        
        # Start sequential pagination from the next page
        sequential_page = predicted_pages + 1
        visited_urls = set()
        
        # Build URL for the next page after predictions
        separator = '&' if '?' in url else '?'
        current_url = f"{url}{separator}p={sequential_page}"
        
        while current_url and current_url not in visited_urls:
            visited_urls.add(current_url)
            
            print(f"\nSequential page {sequential_page}: {current_url[:100]}...")
            
            try:
                response = requests.get(current_url, headers=headers, timeout=30)
                
                # If we get 404, we've reached the end
                if response.status_code == 404:
                    print(f"Page {sequential_page}: 404 - End of results")
                    break
                    
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                articles = soup.find_all('article', class_='item')
                
                if len(articles) == 0:
                    print(f"Page {sequential_page}: No articles found - End of results")
                    break
                
                # Process listings on this page
                page_new = 0
                for article in articles:
                    listing_data = parse_listing(article, current_url)
                    if listing_data:
                        try:
                            if save_listing_to_db(listing_data):
                                page_new += 1
                                new_listings += 1
                            total_listings += 1
                        except Exception as e:
                            print(f"  Error saving listing {listing_data[0]}: {e}")
                
                print(f"Sequential page {sequential_page}: Found {len(articles)} listings, saved {page_new} new")
                
                # Find next page using the existing pagination logic
                next_url = None
                next_arrow = soup.find('a', class_=lambda x: x and 'arrow' in x and 'next' in x)
                if next_arrow and next_arrow.get('href'):
                    potential_next = build_proxy_url(url.split('?')[0], next_arrow.get('href'))
                    if potential_next not in visited_urls:
                        next_url = potential_next
                
                current_url = next_url
                sequential_page += 1
                
                time.sleep(1)  # Respectful delay for sequential pages
                
            except Exception as e:
                print(f"Sequential page {sequential_page}: Error - {e}")
            except Exception as e:
                print(f"Sequential page {sequential_page}: Error - {e}")
                break
    
    print(f"\nBatch {batch_id} completed!")
    print(f"Total listings processed: {total_listings}")
    print(f"New listings saved: {new_listings}")
    print(f"Predicted pages: {predicted_pages}, Total pages processed: {current_page - 1 + (sequential_page - predicted_pages - 1 if 'sequential_page' in locals() else 0)}")
    
    # Update results_found in database
    update_results_found(batch_id, total_listings)
    
    return new_listings


def scrape_single_batch():
    """Scrape the smallest batch from the batch planning table."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    # Get the smallest batch
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, brand_keys, models_keys, results_expected 
        FROM autowereld_batch_planning 
        WHERE results_found = 0 
        ORDER BY results_expected ASC 
        LIMIT 1
    """)
    
    record = cursor.fetchone()
    conn.close()
    
    if not record:
        print("No unprocessed batches found!")
        return
    
    batch_id, brand_keys, models_keys, expected_results = record
    
    # Use the new function
    scrape_single_batch_by_id(batch_id)


if __name__ == "__main__":
    scrape_single_batch()
