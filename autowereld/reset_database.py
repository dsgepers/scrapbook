#!/usr/bin/env python3
"""
Reset batch processing status and clear results table.
"""

import sqlite3
import os


def reset_database():
    """Reset the batch processing status and clear results."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Clear all results
    cursor.execute("DELETE FROM autowereld_results")
    
    # Reset all batch processing status
    cursor.execute("UPDATE autowereld_batch_planning SET results_found = 0")
    
    conn.commit()
    
    # Show status
    cursor.execute("SELECT COUNT(*) FROM autowereld_results")
    results_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM autowereld_batch_planning WHERE results_found = 0")
    unprocessed_batches = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM autowereld_batch_planning")
    total_batches = cursor.fetchone()[0]
    
    print(f"Database reset completed!")
    print(f"Results table: {results_count} records")
    print(f"Batch planning: {unprocessed_batches}/{total_batches} unprocessed")
    
    conn.close()


if __name__ == "__main__":
    reset_database()
