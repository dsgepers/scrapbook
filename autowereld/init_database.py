#!/usr/bin/env python3
"""
Initialize SQLite database with autowereld_batch_planning table.
If the database file already exists, it will be deleted and recreated.
"""

import sqlite3
import os


def init_database():
    """Initialize SQLite database with autowereld_batch_planning table."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "result.db")

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
    create_batch_planning_sql = """
    CREATE TABLE autowereld_batch_planning (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        brand_keys TEXT,
        models_keys TEXT,
        results_expected INTEGER,
        results_found INTEGER
    )
    """

    cursor.execute(create_batch_planning_sql)
    print("Table 'autowereld_batch_planning' created successfully.")

    # Create the autowereld_results table
    create_results_sql = """
    CREATE TABLE autowereld_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identifier TEXT NOT NULL,
        url TEXT NOT NULL,
        licenseplate TEXT,
        construction_year INTEGER,
        mileage INTEGER,
        price INTEGER,
        seller_name TEXT,
        seller_identifier TEXT,
        tags TEXT
    )
    """

    cursor.execute(create_results_sql)
    print("Table 'autowereld_results' created successfully.")

    conn.commit()

    # Verify the tables were created
    batch_planning_query = ("SELECT name FROM sqlite_master WHERE type='table' "
                           "AND name='autowereld_batch_planning'")
    cursor.execute(batch_planning_query)
    batch_planning_result = cursor.fetchone()

    results_query = ("SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name='autowereld_results'")
    cursor.execute(results_query)
    results_result = cursor.fetchone()

    if batch_planning_result and results_result:
        print("Database initialization completed successfully!")

        # Show table structures
        print("\nTable 'autowereld_batch_planning' structure:")
        cursor.execute("PRAGMA table_info(autowereld_batch_planning)")
        columns = cursor.fetchall()
        for column in columns:
            print(f"  {column[1]} ({column[2]})")

        print("\nTable 'autowereld_results' structure:")
        cursor.execute("PRAGMA table_info(autowereld_results)")
        columns = cursor.fetchall()
        for column in columns:
            nullable = "" if column[3] else " (nullable)"
            print(f"  {column[1]} ({column[2]}){nullable}")
    else:
        print("Error: Tables were not created properly.")

    conn.close()


if __name__ == "__main__":
    init_database()
