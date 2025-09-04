#!/usr/bin/env python3
"""
Test version - scrape a single small batch to verify functionality.
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import re
import os
from urllib.parse import urljoin


def test_single_batch():
    """Test scraping with the smallest batch (hyundai i10)."""
    print("Testing scraper with smallest batch: hyundai i10")
    
    # Build the URL for hyundai i10
    url = "https://www.autowereld.nl/zoeken.html?mrk=hyundai&mdl=hyundai_i10&il=100"
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'referer': 'https://www.autowereld.nl/',
        'user-agent': 'Googlebot'
    }
    
    try:
        print(f"Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all article.item elements
        articles = soup.find_all('article', class_='item')
        print(f"Found {len(articles)} article.item elements")
        
        if len(articles) > 0:
            # Test parsing the first article
            first_article = articles[0]
            print("\nTesting parsing of first article:")
            print("HTML structure:")
            print(first_article.prettify()[:1000] + "..." if len(str(first_article)) > 1000 else first_article.prettify())
            
            # Try to parse it
            print("\nParsing results:")
            
            # Extract URL and identifier
            link_elem = first_article.find('a', class_='frame')
            if link_elem:
                relative_url = link_elem.get('href', '')
                full_url = urljoin(url, relative_url)
                print(f"URL: {full_url}")
                
                # Extract identifier
                action_elem = first_article.find('div', {'data-nr': True})
                if action_elem:
                    identifier = action_elem.get('data-nr')
                    print(f"Identifier: {identifier}")
                else:
                    url_match = re.search(r'-(\d+)/', relative_url)
                    if url_match:
                        identifier = url_match.group(1)
                        print(f"Identifier (from URL): {identifier}")
            
            # Extract seller
            seller_elem = first_article.find('div', class_='seller')
            if seller_elem:
                name_elem = seller_elem.find('span', class_='name')
                if name_elem:
                    seller_name = name_elem.get_text(strip=True)
                    print(f"Seller: {seller_name}")
            
            # Extract mileage and year
            mileage_build_elem = first_article.find('span', class_='text-mileage-build')
            if mileage_build_elem:
                text = mileage_build_elem.get_text(strip=True)
                print(f"Mileage/Build text: {text}")
            
            # Extract price
            price_elem = first_article.find('div', class_='price')
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                print(f"Price text: {price_text}")
            
            # Extract specs/tags
            specs_elem = first_article.find('ul', class_='specs')
            if specs_elem:
                tags = []
                for li in specs_elem.find_all('li'):
                    tag_text = li.get_text(strip=True)
                    if tag_text:
                        tags.append(tag_text)
                print(f"Tags: {tags}")
        
        # Check for next page link
        next_link = soup.find('a', class_=['arrow', 'next'])
        if next_link:
            next_url = urljoin(url, next_link.get('href', ''))
            print(f"\nNext page URL: {next_url}")
        else:
            print("\nNo next page found")
            
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_single_batch()
