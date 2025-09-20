#!/usr/bin/env python3
"""
Fetch brand data from autowereld.nl API and extract brand checkboxes with counts.
"""

import requests
import json
import os
import random
from datetime import datetime
from bs4 import BeautifulSoup
import sys

# Add parent directory to path to import Database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.database import Database


def set_github_output(name, value):
    """Set GitHub Actions output variable."""
    github_output = os.getenv('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            f.write(f"{name}={value}\n")
        print(f"Set GitHub output: {name}={value}")
    else:
        print(f"Would set output: {name}={value} (not in GitHub Actions)")


def set_github_env(name, value):
    """Set GitHub Actions environment variable for subsequent steps."""
    github_env = os.getenv('GITHUB_ENV')
    if github_env:
        with open(github_env, 'a') as f:
            f.write(f"{name}={value}\n")
        print(f"Set GitHub environment variable: {name}={value}")
    else:
        print(f"Would set env var: {name}={value} (not in GitHub Actions)")


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


def fetch_brand_data():
    """Fetch brand data from autowereld.nl and parse the response."""
    url = 'https://rez5adtep6.execute-api.eu-central-1.amazonaws.com/fireprox/zoeken.html'
    
    # Parameters for the request
    params = {
        'prvan': '0',
        'prtot': '0',
        'bjvan': '0',
        'bjtot': '0',
        'kmvan': '0',
        'kmtot': '0',
        'pc': '',
        'q': '',
        'filter': 'brand'
    }
    
    # Headers to match the curl request
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'referer': 'https://www.autowereld.nl/',
        'user-agent': 'Googlebot',
        'x-requested-with': 'XMLHttpRequest',
        #'X-My-X-Forwarded-For': get_random_googlebot_ip()
    }
    
    try:
        # Make the HTTP request
        print("Making request to autowereld.nl...")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        if 'html' not in data:
            print("Error: No 'html' key found in response")
            return {}
        
        # Parse the HTML content
        html_content = data['html']
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all checkboxes and their corresponding counts
        brand_data = {}
        
        # Find all checkbox inputs for brands
        checkboxes = soup.find_all('input', {'type': 'checkbox'})
        
        for checkbox in checkboxes:
            brand_value = checkbox.get('value')
            if brand_value:
                # Find the corresponding count span
                # Look for span with class 'count' in the same parent or nearby
                count_span = None
                
                # Try to find count span in the same label or parent element
                parent = checkbox.find_parent()
                if parent:
                    count_span = parent.find('span', class_='count')
                
                # If not found, try looking in the next siblings
                if not count_span:
                    next_elements = checkbox.find_next_siblings()
                    for element in next_elements:
                        count_span = element.find('span', class_='count')
                        if count_span:
                            break
                
                # Extract count value
                count = 0
                if count_span:
                    count_text = count_span.get_text(strip=True)
                    # Extract number from text (remove parentheses, etc.)
                    count_text = count_text.replace('(', '').replace(')', '')
                    # Handle European number format (dots as thousands separators)
                    count_text = count_text.replace('.', '')
                    try:
                        count = int(count_text)
                    except ValueError:
                        print(f"Warning: Could not parse count '{count_text}' for brand '{brand_value}'")
                        count = 0
                
                brand_data[brand_value] = count
                print(f"Brand: {brand_value} -> Count: {count}")
        
        return brand_data
        
    except requests.RequestException as e:
        print(f"Request error: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {}


def fetch_model_data(brand_value):
    """Fetch model data for a specific brand from autowereld.nl."""
    url = 'https://rez5adtep6.execute-api.eu-central-1.amazonaws.com/fireprox/zoeken.html'
    
    # Parameters for the request
    params = {
        'mrk[]': brand_value,
        'prvan': '0',
        'prtot': '0',
        'bjvan': '0',
        'bjtot': '0',
        'kmvan': '0',
        'kmtot': '0',
        'pc': '',
        'q': '',
        'filter': 'model'
    }
    
    # Headers to match the curl request
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'referer': 'https://www.autowereld.nl/',
        'user-agent': 'Googlebot',
        'x-requested-with': 'XMLHttpRequest',
        #'X-My-X-Forwarded-For': get_random_googlebot_ip()
    }
    
    try:
        # Make the HTTP request
        print(f"Fetching models for brand: {brand_value}...")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        if 'html' not in data:
            print(f"Error: No 'html' key found in response for brand {brand_value}")
            return {}
        
        # Parse the HTML content
        html_content = data['html']
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find all checkboxes and their corresponding counts
        model_data = {}
        
        # Find all checkbox inputs for models
        checkboxes = soup.find_all('input', {'type': 'checkbox'})
        
        for checkbox in checkboxes:
            model_value = checkbox.get('value')
            if model_value:
                # Find the corresponding count span
                # Look for span with class 'count' in the same parent or nearby
                count_span = None
                
                # Try to find count span in the same label or parent element
                parent = checkbox.find_parent()
                if parent:
                    count_span = parent.find('span', class_='count')
                
                # If not found, try looking in the next siblings
                if not count_span:
                    next_elements = checkbox.find_next_siblings()
                    for element in next_elements:
                        count_span = element.find('span', class_='count')
                        if count_span:
                            break
                
                # Extract count value
                count = 0
                if count_span:
                    count_text = count_span.get_text(strip=True)
                    # Extract number from text (remove parentheses, etc.)
                    count_text = count_text.replace('(', '').replace(')', '')
                    # Handle European number format (dots as thousands separators)
                    count_text = count_text.replace('.', '')
                    try:
                        count = int(count_text)
                    except ValueError:
                        print(f"Warning: Could not parse count '{count_text}' for model '{model_value}' in brand '{brand_value}'")
                        count = 0
                
                model_data[model_value] = count
                print(f"  Model: {model_value} -> Count: {count}")
        
        return model_data
        
    except requests.RequestException as e:
        print(f"Request error for brand {brand_value}: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"JSON decode error for brand {brand_value}: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error for brand {brand_value}: {e}")
        return {}


def group_models_by_limit(model_data, max_limit=9000):
    """Group models so that no group exceeds the maximum limit."""
    if not model_data:
        return []
    
    grouped_models = []
    current_group = []
    current_count = 0
    
    # Sort models by count (ascending) to optimize grouping
    sorted_models = sorted(model_data.items(), key=lambda x: x[1])
    
    for model, count in sorted_models:
        # If a single model exceeds the limit, it gets its own group
        if count > max_limit:
            grouped_models.append({
                "models": model,
                "count": count
            })
            print(f"    Large model (standalone): {model} -> {count}")
            continue
        
        # If adding this model would exceed the limit, finalize current group
        if current_count + count > max_limit and current_group:
            grouped_models.append({
                "models": '|'.join(current_group),
                "count": current_count
            })
            print(f"    Model group: {'|'.join(current_group)} -> {current_count}")
            
            # Start new group with current model
            current_group = [model]
            current_count = count
        else:
            # Add model to current group
            current_group.append(model)
            current_count += count
    
    # Don't forget the last group
    if current_group:
        grouped_models.append({
            "models": '|'.join(current_group),
            "count": current_count
        })
        print(f"    Model group: {'|'.join(current_group)} -> {current_count}")
    
    return grouped_models


def group_brands_by_limit(brand_data, max_limit=9000):
    """Group brands so that no group exceeds the maximum limit."""
    grouped_brands = []
    current_group = []
    current_count = 0
    
    # Sort brands by count (ascending) to optimize grouping
    sorted_brands = sorted(brand_data.items(), key=lambda x: x[1])
    
    for brand, count in sorted_brands:
        # If a single brand exceeds the limit, it gets its own group
        if count > max_limit:
            print(f"Large brand (standalone): {brand} -> {count}")
            
            # Fetch models for this brand and group them
            model_data = fetch_model_data(brand)
            grouped_models = group_models_by_limit(model_data, max_limit)
            
            # Create separate entries for each model group
            for model_group in grouped_models:
                group_obj = {
                    "brands": brand,
                    "models": model_group["models"],
                    "count": model_group["count"]
                }
                grouped_brands.append(group_obj)
                print(f"  Added model group for {brand}: {model_group['models']} -> {model_group['count']}")
            
            continue
        
        # If adding this brand would exceed the limit, finalize current group
        if current_count + count > max_limit and current_group:
            group_obj = {
                "brands": '|'.join(current_group),
                "models": "",
                "count": current_count
            }
            grouped_brands.append(group_obj)
            print(f"Group: {'|'.join(current_group)} -> {current_count}")
            
            # Start new group with current brand
            current_group = [brand]
            current_count = count
        else:
            # Add brand to current group
            current_group.append(brand)
            current_count += count
    
    # Don't forget the last group
    if current_group:
        group_obj = {
            "brands": '|'.join(current_group),
            "models": "",
            "count": current_count
        }
        grouped_brands.append(group_obj)
        print(f"Group: {'|'.join(current_group)} -> {current_count}")
    
    return grouped_brands


def create_batch():
    """Create a new batch record in autowereld_batch table and return the batch_id."""
    try:
        db = Database()
        db.connect()
        cursor = db.connection.cursor()
        
        # Insert new batch with current timestamp (id should auto-increment)
        current_time = datetime.now()
        cursor.execute("""
            INSERT INTO autowereld_batch (created_at)
            VALUES (%s)
        """, (current_time,))
        
        # Get the auto-generated batch_id
        batch_id = cursor.lastrowid
        db.connection.commit()
        
        print(f"Created new batch with ID: {batch_id}")
        
        # Set the batch ID as GitHub Actions environment variable and output
        set_github_env('BATCH_ID', str(batch_id))
        set_github_output('batch_id', str(batch_id))
        
        db.close()
        return batch_id
        
    except Exception as e:
        print(f"Error creating batch: {e}")
        if 'db' in locals():
            db.close()
        
        # If AUTO_INCREMENT is not working, try a different approach
        try:
            print("Attempting alternative approach with explicit ID...")
            db = Database()
            db.connect()
            cursor = db.connection.cursor()
            
            # Get the max ID and increment it
            cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM autowereld_batch")
            next_id = cursor.fetchone()[0]
            
            current_time = datetime.now()
            cursor.execute("""
                INSERT INTO autowereld_batch (id, created_at)
                VALUES (%s, %s)
            """, (next_id, current_time))
            
            db.connection.commit()
            
            print(f"Created new batch with explicit ID: {next_id}")
            
            # Set the batch ID as GitHub Actions environment variable and output
            set_github_env('BATCH_ID', str(next_id))
            set_github_output('batch_id', str(next_id))
            
            db.close()
            return next_id
            
        except Exception as e2:
            print(f"Alternative approach also failed: {e2}")
            if 'db' in locals():
                db.close()
            return None


def save_to_database(grouped_brands):
    """Save grouped brands data to MySQL database using Database class."""
    # First create a new batch
    batch_id = create_batch()
    if batch_id is None:
        print("Failed to create batch. Cannot proceed with saving data.")
        return False
    
    try:
        db = Database()
        db.connect()
        cursor = db.connection.cursor()
        
        # Insert new data into autowereld_batch_plannings
        # Note: Work around foreign key constraint by setting planning ID = batch ID
        inserted_count = 0
        for i, group_obj in enumerate(grouped_brands):
            try:
                # Use batch_id + i as the planning ID to work around the constraint
                planning_id = batch_id if i == 0 else batch_id * 1000 + i
                cursor.execute("""
                    INSERT INTO autowereld_batch_plannings 
                    (id, batch_id, brand_keys, models_keys, results_expected, results_found)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    planning_id,
                    batch_id,
                    group_obj['brands'],
                    group_obj['models'],
                    group_obj['count'],
                    0  # results_found starts at 0
                ))
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting planning record {inserted_count + 1}: {e}")
                print(f"Batch ID: {batch_id}, Brands: {group_obj['brands'][:50]}...")
                # Continue with next record
                continue
        
        db.connection.commit()
        
        print(f"Successfully inserted {inserted_count} records into the database for batch {batch_id}.")
        
        # Show some sample records
        cursor.execute("""
            SELECT id, brand_keys, models_keys, results_expected, results_found 
            FROM autowereld_batch_plannings 
            WHERE batch_id = %s 
            LIMIT 5
        """, (batch_id,))
        sample_records = cursor.fetchall()
        
        print("\nSample database records:")
        print("ID | Brand Keys | Model Keys | Expected | Found")
        print("-" * 80)
        for record in sample_records:
            brand_keys = record[1][:30] + "..." if len(record[1]) > 30 else record[1]
            model_keys = record[2][:30] + "..." if len(record[2]) > 30 else record[2]
            print(f"{record[0]:2} | {brand_keys:<30} | {model_keys:<30} | {record[3]:8} | {record[4]:5}")
        
        # Show total count for this batch
        cursor.execute("SELECT COUNT(*) FROM autowereld_batch_plannings WHERE batch_id = %s", (batch_id,))
        total_count = cursor.fetchone()[0]
        print(f"\nTotal records in database for batch {batch_id}: {total_count}")
        
        # Show total expected results for this batch
        cursor.execute("SELECT SUM(results_expected) FROM autowereld_batch_plannings WHERE batch_id = %s", (batch_id,))
        total_expected = cursor.fetchone()[0]
        print(f"Total expected results for batch {batch_id}: {total_expected}")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"Database error: {e}")
        if 'db' in locals():
            db.close()
        return False


def main():
    """Main function to fetch and display brand data."""
    print("Fetching brand data from autowereld.nl...")
    brand_data = fetch_brand_data()
    
    if brand_data:
        print(f"\nFound {len(brand_data)} brands:")
        print("=" * 50)
        
        # Sort by count (descending) and display
        sorted_brands = sorted(brand_data.items(), key=lambda x: x[1], reverse=True)
        
        for brand, count in sorted_brands:
            print(f"{brand:<30} {count:>6} cars")
        
        print("=" * 50)
        print(f"Total brands: {len(brand_data)}")
        
        # Group brands by limit
        print("\n" + "=" * 50)
        print("GROUPING BRANDS (Max 9000 per group)")
        print("=" * 50)
        
        grouped_brands = group_brands_by_limit(brand_data, 9000)
        
        print("\n" + "=" * 50)
        print("FINAL GROUPED RESULTS")
        print("=" * 50)
        
        # Sort grouped results by count (descending)
        sorted_groups = sorted(grouped_brands, key=lambda x: x['count'], reverse=True)
        
        for i, group_obj in enumerate(sorted_groups, 1):
            brand_count = len(group_obj['brands'].split('|'))
            print(f"{i:2}. Brands: {brand_count:<2} | Count: {group_obj['count']:>6} | Keys: {group_obj['brands']}")
        
        print("=" * 50)
        print(f"Total groups: {len(grouped_brands)}")
        
        # Save grouped data to MySQL database
        print("\n" + "=" * 50)
        print("SAVING TO DATABASE")
        print("=" * 50)
        
        success = save_to_database(sorted_groups)
        
        if success:
            print("Data successfully saved to database!")
        else:
            print("Failed to save data to database.")
        
    else:
        print("No brand data found or error occurred.")


if __name__ == "__main__":
    main()
