#!/usr/bin/env python3
"""
Test version - scrape one small batch and save to database.
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import re
import os
from urllib.parse import urljoin


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
    
    cursor.execute("""
        INSERT INTO autowereld_results 
        (identifier, url, licenseplate, construction_year, mileage, price, 
         seller_name, seller_identifier, tags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, listing_data)
    
    conn.commit()
    conn.close()


def test_batch_scraping():
    """Test scraping one batch and saving to database."""
    print("Testing batch scraping with hyundai i10...")
    
    # Clear any existing test data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM autowereld_results")
    conn.commit()
    conn.close()
    
    # Build the URL for hyundai i10
    url = "https://www.autowereld.nl/zoeken.html?mrk=hyundai&mdl=hyundai_i10&il=100"
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'referer': 'https://www.autowereld.nl/',
        'user-agent': 'Googlebot'
    }
    
    current_url = url
    total_listings = 0
    page_count = 0
    
    while current_url and page_count < 3:  # Limit to 3 pages for testing
        page_count += 1
        print(f"\nScraping page {page_count}: {current_url}")
        
        try:
            response = requests.get(current_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all article.item elements
            articles = soup.find_all('article', class_='item')
            print(f"Found {len(articles)} listings on this page")
            
            page_listings = 0
            for article in articles:
                listing_data = parse_listing(article, current_url)
                if listing_data:
                    try:
                        save_listing_to_db(listing_data)
                        page_listings += 1
                        total_listings += 1
                        
                        # Print first few for verification
                        if total_listings <= 3:
                            print(f"  Saved listing {listing_data[0]}: {listing_data[1][:100]}...")
                            print(f"    Mileage: {listing_data[4]}, Year: {listing_data[3]}, Price: {listing_data[5]}")
                            print(f"    Seller: {listing_data[6]}, Tags: {listing_data[8]}")
                        
                    except Exception as e:
                        print(f"  Error saving listing {listing_data[0]}: {e}")
            
            print(f"Saved {page_listings} listings from this page")
            
            # Find next page link
            next_link = soup.find('a', class_=['arrow', 'next'])
            if next_link and next_link.get('href'):
                current_url = urljoin(current_url, next_link.get('href'))
            else:
                print("No next page found")
                break
                
            time.sleep(1)  # Be respectful
            
        except Exception as e:
            print(f"Error scraping page: {e}")
            break
    
    print(f"\nTest completed!")
    print(f"Total listings saved: {total_listings}")
    print(f"Pages scraped: {page_count}")
    
    # Show some database results
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM autowereld_results")
    count = cursor.fetchone()[0]
    print(f"Records in database: {count}")
    
    cursor.execute("SELECT identifier, construction_year, mileage, price, seller_name FROM autowereld_results LIMIT 5")
    records = cursor.fetchall()
    
    print("\nFirst 5 database records:")
    print("ID | Year | Mileage | Price | Seller")
    print("-" * 50)
    for record in records:
        print(f"{record[0]} | {record[1] or 'N/A'} | {record[2] or 'N/A'} | {record[3] or 'N/A'} | {record[4] or 'N/A'}")
    
    conn.close()


if __name__ == "__main__":
    test_batch_scraping()
