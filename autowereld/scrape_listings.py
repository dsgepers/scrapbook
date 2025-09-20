#!/usr/bin/env python3
"""
Scrape vehicle listings from autowereld.nl based on batch planning records.
This script processes all unprocessed batches by calling the single batch scraper.
"""

import os
import time
import sys
from scrape_single_batch import scrape_single_batch_by_id

# Add parent directory to path to import Database class
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.database import Database


def get_unprocessed_batches_for_batch_id(batch_id):
    """Get all unprocessed planning records for a specific batch ID."""
    db = Database()
    db.connect()
    cursor = db.connection.cursor()
    
    cursor.execute("""
        SELECT id, brand_keys, results_expected 
        FROM autowereld_batch_plannings 
        WHERE batch_id = %s AND results_found = 0
        ORDER BY results_expected ASC
    """, (batch_id,))
    
    records = cursor.fetchall()
    db.close()
    
    return records


def get_unprocessed_batches():
    """Get all unprocessed batch IDs ordered by expected results (smallest first)."""
    db = Database()
    db.connect()
    cursor = db.connection.cursor()
    
    cursor.execute("""
        SELECT id, brand_keys, results_expected 
        FROM autowereld_batch_plannings 
        WHERE results_found = 0
        ORDER BY results_expected ASC
    """)
    
    records = cursor.fetchall()
    db.close()
    
    return records


def main():
    """Main scraping function that processes batches."""
    print("Starting autowereld listings scraper...")
    
    # Check if a specific batch ID is provided via environment variable
    batch_id_env = os.getenv('BATCH_ID')
    
    if batch_id_env:
        try:
            batch_id = int(batch_id_env)
            print(f"Processing specific batch ID from environment: {batch_id}")
            
            # Get all unprocessed planning records for this batch
            planning_records = get_unprocessed_batches_for_batch_id(batch_id)
            
            if not planning_records:
                print(f"No unprocessed planning records found for batch {batch_id}")
                return
            
            print(f"Found {len(planning_records)} unprocessed planning records for batch {batch_id}")
            
            total_scraped = 0
            for i, (planning_id, brand_keys, expected_results) in enumerate(planning_records, 1):
                print(f"\nProcessing planning record {i}/{len(planning_records)} (ID: {planning_id}) - {brand_keys}")
                print(f"Expected results: {expected_results:,}")
                
                scraped_count = scrape_single_batch_by_id(planning_id)
                total_scraped += scraped_count
                
                print(f"Planning record {planning_id} completed: {scraped_count:,} listings")
                
                if i < len(planning_records):
                    time.sleep(0.2)
            
            print(f"Batch {batch_id} completed: {total_scraped:,} total listings")
            return
            
        except ValueError:
            print(f"Invalid BATCH_ID environment variable: {batch_id_env}")
            print("Falling back to processing all unprocessed batches...")
        except Exception as e:
            print(f"Error processing batch {batch_id}: {e}")
            return
    
    # Fallback: Get all unprocessed batch planning records
    batches = get_unprocessed_batches()
    print(f"Found {len(batches)} unprocessed batch planning records")
    # Fallback: Get all unprocessed batch planning records
    batches = get_unprocessed_batches()
    print(f"Found {len(batches)} unprocessed batch planning records")
    
    if len(batches) == 0:
        print("No unprocessed batches found. All batches have been completed!")
        return
    
    # Show total expected results
    total_expected = sum(batch[2] for batch in batches)
    print(f"Total expected results to process: {total_expected:,}")
    
    total_scraped = 0
    
    for i, (planning_id, brand_keys, expected_results) in enumerate(batches, 1):
        try:
            print(f"\n{'='*60}")
            print(f"Processing batch {i}/{len(batches)} (Planning ID: {planning_id}) - {brand_keys}")
            print(f"Expected results: {expected_results:,}")
            print(f"{'='*60}")
            
            # Call the working single batch scraper
            scraped_count = scrape_single_batch_by_id(planning_id)
            total_scraped += scraped_count
            
            print(f"Planning record {planning_id} completed: {scraped_count:,} listings")
            print(f"Progress: {i}/{len(batches)} batches ({(i/len(batches)*100):.1f}%)")
            print(f"Total scraped so far: {total_scraped:,}")
            
            # Delay between batches
            if i < len(batches):  # Don't delay after the last batch
                print("Waiting 0.2 seconds before next batch...")
                time.sleep(0.2)
            
        except KeyboardInterrupt:
            print(f"\nScraping interrupted by user!")
            print(f"Completed {i-1}/{len(batches)} batches")
            print(f"Total listings scraped: {total_scraped:,}")
            break
        except Exception as e:
            print(f"Error processing planning record {planning_id}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"Scraping completed!")
    print(f"Processed {len(batches)} planning records")
    print(f"Total listings scraped: {total_scraped:,}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
