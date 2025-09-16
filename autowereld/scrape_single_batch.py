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
    
    current_url = url
    total_listings = 0
    new_listings = 0
    page_count = 0
    visited_urls = set()  # Track visited URLs to prevent loops
    
    while current_url and current_url not in visited_urls:
        visited_urls.add(current_url)
        page_count += 1
        print(f"\nScraping page {page_count}: {current_url[:100]}...")
        
        try:
            response = requests.get(current_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all article.item elements
            articles = soup.find_all('article', class_='item')
            print(f"Found {len(articles)} listings on this page")
            
            # If no articles found, we might have reached the end
            if len(articles) == 0:
                print("No articles found on this page - stopping")
                break
            
            page_listings = 0
            for article in articles:
                listing_data = parse_listing(article, current_url)
                if listing_data:
                    try:
                        if save_listing_to_db(listing_data):
                            page_listings += 1
                            new_listings += 1
                        total_listings += 1
                        
                    except Exception as e:
                        print(f"  Error saving listing {listing_data[0]}: {e}")
            
            print(f"Saved {page_listings} new listings from this page (total processed: {total_listings})")
            
            # Find next page link - check fresh on each page
            next_url = None
            
            # Method 1: Look for arrow.next link (most reliable)
            # Be more specific - we want 'next' class, not 'prev'
            next_arrow = soup.find('a', class_=lambda x: x and 'arrow' in x and 'next' in x)
            if next_arrow and next_arrow.get('href'):
                potential_next = build_proxy_url(base_url, next_arrow.get('href'))
                # Make sure it's not a URL we've already visited
                if potential_next not in visited_urls:
                    next_url = potential_next
                    print(f"Found next page: {next_url}")
                else:
                    print(f"Next page already visited: {potential_next}")
            
            # Method 2: If no arrow.next, look for pagination with numeric pages
            if not next_url:
                pagination = soup.find('div', class_=['pagination', 'pager', 'pages'])
                if pagination:
                    # Look for current page number and calculate next
                    current_page_elem = pagination.find('span', class_='current') or pagination.find('strong')
                    if current_page_elem:
                        try:
                            current_page_num = int(current_page_elem.get_text(strip=True))
                            next_page_num = current_page_num + 1
                            
                            # Look for next page link
                            for link in pagination.find_all('a'):
                                if link.get_text(strip=True) == str(next_page_num):
                                    potential_next = build_proxy_url(base_url, link.get('href'))
                                    if potential_next not in visited_urls:
                                        next_url = potential_next
                                        print(f"Found pagination page {next_page_num}: {next_url}")
                                    break
                        except ValueError:
                            pass
            
            # Method 3: Look for "Volgende" or "Next" text links
            if not next_url:
                for link in soup.find_all('a'):
                    text = link.get_text(strip=True).lower()
                    if text in ['volgende', 'next', '>', '→'] and link.get('href'):
                        potential_next = build_proxy_url(base_url, link.get('href'))
                        if potential_next not in visited_urls:
                            next_url = potential_next
                            print(f"Found text-based next link: {next_url}")
                            break
            
            # Check if we found a valid next URL
            if next_url:
                current_url = next_url
            else:
                print("No more pages found - pagination complete")
                break
                
            time.sleep(0.3)  # Be respectful
            
        except Exception as e:
            print(f"Error scraping page: {e}")
            break
    
    print(f"\nBatch {batch_id} completed!")
    print(f"Total listings processed: {total_listings}")
    print(f"New listings saved: {new_listings}")
    print(f"Pages scraped: {page_count}")
    
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
