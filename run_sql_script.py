#!/usr/bin/env python3
"""
Data Warehouse Lab - SQL Query Execution Script

This script allows students to execute SQL queries on CSV data using DuckDB.
It loads CSV files as tables, runs SQL queries, and logs the results.

Usage:
    python run_sql_script.py <path_to_sql_file> <path_to_data_folder> <path_to_log_file>

Example:
    python run_sql_script.py queries/sample_schema/cube_example.sql data/sample_schema/ logs/run1.txt

Author: Data Warehouse Teaching Lab
"""

import sys
import os
import logging
import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime


def setup_logging(log_file_path):
    """
    Set up logging configuration to write to both file and console.
    
    Args:
        log_file_path (str): Path to the log file
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, mode='w'),
            logging.StreamHandler()
        ]
    )


def validate_arguments(sql_file_path, data_folder_path, log_file_path):
    """
    Validate command line arguments and check if files/folders exist.
    
    Args:
        sql_file_path (str): Path to SQL file
        data_folder_path (str): Path to data folder
        log_file_path (str): Path to log file
    
    Returns:
        bool: True if all arguments are valid, False otherwise
    """
    # Check if SQL file exists
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file '{sql_file_path}' not found.")
        return False
    
    # Check if SQL file has .sql extension
    if not sql_file_path.lower().endswith('.sql'):
        print(f"Error: '{sql_file_path}' is not a SQL file (.sql extension required).")
        return False
    
    # Check if data folder exists
    if not os.path.exists(data_folder_path):
        print(f"Error: Data folder '{data_folder_path}' not found.")
        return False
    
    if not os.path.isdir(data_folder_path):
        print(f"Error: '{data_folder_path}' is not a directory.")
        return False
    
    return True


def load_csv_files(data_folder_path, conn):
    """
    Load all CSV files from the data folder into DuckDB as tables.
    
    Args:
        data_folder_path (str): Path to folder containing CSV files
        conn: DuckDB connection object
    
    Returns:
        list: List of loaded table names
    """
    loaded_tables = []
    csv_files = []
    
    # Find all CSV files in the data folder
    for file in os.listdir(data_folder_path):
        if file.lower().endswith('.csv'):
            csv_files.append(file)
    
    if not csv_files:
        logging.warning(f"No CSV files found in '{data_folder_path}'")
        return loaded_tables
    
    logging.info(f"Found {len(csv_files)} CSV file(s) in '{data_folder_path}'")
    
    # Load each CSV file as a table
    for csv_file in csv_files:
        try:
            csv_path = os.path.join(data_folder_path, csv_file)
            table_name = os.path.splitext(csv_file)[0]  # Remove .csv extension
            
            # Load CSV into pandas DataFrame first for better error handling
            df = pd.read_csv(csv_path)
            
            # Register the DataFrame as a table in DuckDB
            conn.register(table_name, df)
            
            logging.info(f"✓ Loaded '{csv_file}' as table '{table_name}' ({len(df)} rows, {len(df.columns)} columns)")
            loaded_tables.append(table_name)
            
        except Exception as e:
            logging.error(f"✗ Failed to load '{csv_file}': {str(e)}")
    
    return loaded_tables


def read_sql_file(sql_file_path):
    """
    Read SQL query from file.
    
    Args:
        sql_file_path (str): Path to SQL file
    
    Returns:
        str: SQL query content, or None if error
    """
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
            sql_query = file.read().strip()
        
        if not sql_query:
            logging.error(f"SQL file '{sql_file_path}' is empty")
            return None
        
        logging.info(f"✓ Successfully read SQL file '{sql_file_path}'")
        return sql_query
    
    except Exception as e:
        logging.error(f"✗ Failed to read SQL file '{sql_file_path}': {str(e)}")
        return None


def execute_sql_query(conn, sql_query):
    """
    Execute SQL query using DuckDB.
    
    Args:
        conn: DuckDB connection object
        sql_query (str): SQL query to execute
    
    Returns:
        pandas.DataFrame: Query results, or None if error
    """
    try:
        logging.info("Executing SQL query...")
        logging.info(f"Query:\n{sql_query}\n")
        
        # Execute query and return results as DataFrame
        result_df = conn.execute(sql_query).df()
        
        logging.info(f"✓ Query executed successfully. Result: {len(result_df)} rows, {len(result_df.columns)} columns")
        return result_df
    
    except Exception as e:
        logging.error(f"✗ Query execution failed: {str(e)}")
        return None


def save_results_to_csv(result_df, sql_file_path):
    """
    Save query results to CSV file in the outputs folder, maintaining schema structure.
    
    Args:
        result_df (pandas.DataFrame): Query results
        sql_file_path (str): Path to original SQL file
    
    Returns:
        str: Path to output CSV file, or None if error
    """
    try:
        # Extract relative path structure from queries/ to maintain schema organization
        if sql_file_path.startswith('queries/'):
            # Remove 'queries/' prefix and get the schema path
            relative_path = sql_file_path[8:]  # Remove 'queries/'
            sql_file_base = os.path.splitext(relative_path)[0]
            output_csv_path = f"outputs/{sql_file_base}_output.csv"
        else:
            # Fallback for files not in queries/ structure
            sql_file_base = os.path.splitext(os.path.basename(sql_file_path))[0]
            output_csv_path = f"outputs/{sql_file_base}_output.csv"
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_csv_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save DataFrame to CSV
        result_df.to_csv(output_csv_path, index=False)
        
        logging.info(f"✓ Query results saved to '{output_csv_path}'")
        return output_csv_path
    
    except Exception as e:
        logging.error(f"✗ Failed to save results to CSV: {str(e)}")
        return None


def log_results_preview(result_df, max_rows=10):
    """
    Log a preview of the query results (first few rows).
    
    Args:
        result_df (pandas.DataFrame): Query results
        max_rows (int): Maximum number of rows to display
    """
    logging.info(f"\n=== QUERY RESULTS PREVIEW (First {min(max_rows, len(result_df))} rows) ===")
    
    if len(result_df) == 0:
        logging.info("No rows returned by the query.")
        return
    
    # Display column names
    logging.info(f"Columns: \n{list(result_df.columns)}")
    logging.info("")
    
    # Display first few rows
    preview_df = result_df.head(max_rows)
    
    # Convert to string for logging (with better formatting)
    preview_str = preview_df.to_string(index=False, max_cols=None, max_colwidth=50)
    logging.info(f"\n{preview_str}")
    
    if len(result_df) > max_rows:
        logging.info(f"\n... ({len(result_df) - max_rows} more rows not shown)")
    
    logging.info("=" * 50)


def main():
    """
    Main function to orchestrate the SQL query execution process.
    """
    # Check command line arguments
    if len(sys.argv) != 4:
        print("Usage: python run_sql_script.py <path_to_sql_file> <path_to_data_folder> <path_to_log_file>")
        print("\nExample:")
        print("python run_sql_script.py queries/sample_schema/cube_example.sql data/sample_schema/ logs/run1.txt")
        sys.exit(1)
    
    # Parse command line arguments
    sql_file_path = sys.argv[1]
    data_folder_path = sys.argv[2]
    log_file_path = sys.argv[3]
    
    # Validate arguments
    if not validate_arguments(sql_file_path, data_folder_path, log_file_path):
        sys.exit(1)
    
    # Set up logging
    setup_logging(log_file_path)
    
    # Log execution start
    logging.info("=" * 60)
    logging.info("DATA WAREHOUSE LAB - SQL QUERY EXECUTION")
    logging.info("=" * 60)
    logging.info(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"SQL file: {sql_file_path}")
    logging.info(f"Data folder: {data_folder_path}")
    logging.info(f"Log file: {log_file_path}")
    logging.info("=" * 60)
    
    try:
        # Create DuckDB connection
        conn = duckdb.connect(':memory:')  # In-memory database
        logging.info("✓ Connected to DuckDB")
        
        # Load CSV files as tables
        loaded_tables = load_csv_files(data_folder_path, conn)
        
        if not loaded_tables:
            logging.error("No tables were loaded successfully. Cannot proceed.")
            sys.exit(1)
        
        logging.info(f"✓ Successfully loaded {len(loaded_tables)} table(s): {', '.join(loaded_tables)}")
        
        # Read SQL query from file
        sql_query = read_sql_file(sql_file_path)
        if sql_query is None:
            sys.exit(1)
        
        # Execute SQL query
        result_df = execute_sql_query(conn, sql_query)
        if result_df is None:
            sys.exit(1)
        
        # Log results preview
        log_results_preview(result_df)
        
        # Save results to CSV
        output_csv_path = save_results_to_csv(result_df, sql_file_path)
        if output_csv_path is None:
            sys.exit(1)
        
        # Final success message
        logging.info("=" * 60)
        logging.info("EXECUTION COMPLETED SUCCESSFULLY")
        logging.info(f"Results saved to: {output_csv_path}")
        logging.info(f"Logs saved to: {log_file_path}")
        logging.info("=" * 60)
        
        # Print success message to console
        print(f"Query executed successfully. Results logged to {log_file_path}")
        
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        print(f"Error: Query execution failed. Check {log_file_path} for details.")
        sys.exit(1)
    
    finally:
        # Close DuckDB connection
        if 'conn' in locals():
            conn.close()
            logging.info("✓ DuckDB connection closed")


if __name__ == "__main__":
    main()