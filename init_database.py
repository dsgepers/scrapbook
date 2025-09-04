#!/usr/bin/env python3
"""
Initialize SQLite database with autowereld_batch_planning table.
If the database file already exists, it will be deleted and recreated.
"""

import sqlite3
import os


def init_database():
    """Initialize the SQLite database with the autowereld_batch_planning table."""
    db_path = "result.db"
    
    # Delete the database file if it already exists
    if os.path.exists(db_path):
        print(f"Database file '{db_path}' already exists. Deleting it...")
        os.remove(db_path)
        print(f"Database file '{db_path}' deleted.")
    
    # Create new database connection
    print(f"Creating new database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create the autowereld_batch_planning table
    create_table_sql = """
    CREATE TABLE autowereld_batch_planning (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        brand_keys TEXT,
        models_keys TEXT,
        results_expected INTEGER,
        results_found INTEGER
    )
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    
    print("Table 'autowereld_batch_planning' created successfully.")
    
    # Verify the table was created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='autowereld_batch_planning'")
    result = cursor.fetchone()
    
    if result:
        print("Database initialization completed successfully!")
        
        # Show table structure
        cursor.execute("PRAGMA table_info(autowereld_batch_planning)")
        columns = cursor.fetchall()
        print("\nTable structure:")
        for column in columns:
            print(f"  {column[1]} ({column[2]})")
    else:
        print("Error: Table was not created properly.")
    
    conn.close()


if __name__ == "__main__":
    init_database()
