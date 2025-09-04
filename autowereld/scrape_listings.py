#!/usr/bin/env python3
"""
Scrape vehicle listings from autowereld.nl based on batch planning records.
This script processes all unprocessed batches by calling the single batch scraper.
"""

import sqlite3
import os
import time
from scrape_single_batch import scrape_single_batch_by_id


def get_unprocessed_batches():
    """Get all unprocessed batch IDs ordered by expected results (smallest first)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, brand_keys, results_expected 
        FROM autowereld_batch_planning 
        WHERE results_found = 0
        ORDER BY results_expected ASC
    """)
    
    records = cursor.fetchall()
    conn.close()
    
    return records


def main():
    """Main scraping function that processes all unprocessed batches."""
    print("Starting autowereld listings scraper...")
    
    # Get all unprocessed batch planning records
    batches = get_unprocessed_batches()
    print(f"Found {len(batches)} unprocessed batch planning records")
    
    if len(batches) == 0:
        print("No unprocessed batches found. All batches have been completed!")
        return
    
    # Show total expected results
    total_expected = sum(batch[2] for batch in batches)
    print(f"Total expected results to process: {total_expected:,}")
    
    total_scraped = 0
    
    for i, (batch_id, brand_keys, expected_results) in enumerate(batches, 1):
        try:
            print(f"\n{'='*60}")
            print(f"Processing batch {i}/{len(batches)} (ID: {batch_id}) - {brand_keys}")
            print(f"Expected results: {expected_results:,}")
            print(f"{'='*60}")
            
            # Call the working single batch scraper
            scraped_count = scrape_single_batch_by_id(batch_id)
            total_scraped += scraped_count
            
            print(f"Batch {batch_id} completed: {scraped_count:,} listings")
            print(f"Progress: {i}/{len(batches)} batches ({(i/len(batches)*100):.1f}%)")
            print(f"Total scraped so far: {total_scraped:,}")
            
            # Delay between batches
            if i < len(batches):  # Don't delay after the last batch
                print("Waiting 2 seconds before next batch...")
                time.sleep(2)
            
        except KeyboardInterrupt:
            print(f"\nScraping interrupted by user!")
            print(f"Completed {i-1}/{len(batches)} batches")
            print(f"Total listings scraped: {total_scraped:,}")
            break
        except Exception as e:
            print(f"Error processing batch {batch_id}: {e}")
            continue
    
    print(f"\n{'='*60}")
    print(f"Scraping completed!")
    print(f"Processed {len(batches)} batches")
    print(f"Total listings scraped: {total_scraped:,}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
