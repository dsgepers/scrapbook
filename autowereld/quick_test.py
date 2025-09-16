#!/usr/bin/env python3
"""
Quick test - scrape just 2 pages of the smallest batch.
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import random
from urllib.parse import urljoin


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


def quick_test():
    """Quick test with just 2 pages."""
    print("Quick test: scraping 2 pages of hyundai i10...")
    
    base_url = "https://rez5adtep6.execute-api.eu-central-1.amazonaws.com/fireprox/"
    url = base_url + "zoeken.html?mrk=hyundai&mdl=hyundai_i10&il=100"
    
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
    
    while current_url and page_count < 2:  # Limit to 2 pages
        page_count += 1
        print(f"\nScraping page {page_count}")
        
        try:
            response = requests.get(current_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all article.item elements
            articles = soup.find_all('article', class_='item')
            print(f"Found {len(articles)} listings on this page")
            
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
            
            print(f"Saved {page_new} new listings from this page")
            
            # Find next page link
            next_link = soup.find('a', class_=['arrow', 'next'])
            if next_link and next_link.get('href'):
                href = next_link.get('href')
                print(f"Original href: {href}")
                current_url = build_proxy_url(base_url, href)
                print(f"Next page: {current_url[:80]}...")
            else:
                print("No next page found")
                break
                
            time.sleep(1)
            
        except Exception as e:
            print(f"Error scraping page: {e}")
            break
    
    print(f"\nQuick test completed!")
    print(f"Total listings processed: {total_listings}")
    print(f"New listings saved: {new_listings}")
    print(f"Pages scraped: {page_count}")
    
    # Update batch planning
    if new_listings > 0:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "result.db")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE autowereld_batch_planning SET results_found = ? WHERE id = 48", (total_listings,))
        conn.commit()
        conn.close()
        
        print(f"Updated batch 48 results_found to {total_listings}")


if __name__ == "__main__":
    quick_test()
