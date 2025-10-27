# Data Warehouse Lab - SQL Query Execution Project

## Project Overview

This project provides a hands-on learning environment for students to practice data warehouse concepts by executing SQL queries on CSV data using DuckDB. The project allows you to:

- Load CSV files as database tables
- Execute SQL queries including advanced operations like CUBE, JOINs, and aggregations
- Capture and log query results automatically
- Practice essential data warehouse querying techniques

**Key Learning Objectives:**
- Understanding fact and dimension table relationships
- Writing complex SQL queries with multiple JOINs
- Using CUBE operations for multi-dimensional analysis
- Performing aggregations and analytical functions
- Working with real-world data warehouse scenarios

## Folder Structure

```
data_warehouse_lab/
│
├── run_sql_script.py       # Main Python script for executing SQL queries
├── requirements.txt        # Python dependencies
├── README.md              # This documentation file
│
├── data/                  # Data storage folder
│   └── sample_schema/     # Sample dataset for learning
│       ├── dim_customer.csv    # Customer dimension table
│       ├── dim_product.csv     # Product dimension table
│       └── fact_sales.csv      # Sales fact table
│
├── queries/               # SQL query examples and storage
│   └── sample_schema/     # Queries for the sample schema dataset
│       ├── cube_example.sql        # Demonstrates CUBE operations
│       ├── join_example.sql        # Shows JOIN operations
│       └── aggregation_example.sql # Aggregation functions example
│
├── outputs/               # Query results (auto-generated)
│   └── sample_schema/     # Results organized by schema
│       └── (CSV output files will be created here)
│
└── logs/                  # Query execution logs (auto-generated)
    └── (log files will be created here)
```

### Folder Descriptions

- **`data/`**: Contains CSV files organized by schema that will be loaded as database tables
- **`queries/`**: Store your SQL query files here, organized by schema (must have `.sql` extension)
- **`outputs/`**: Query results are automatically saved here as CSV files, maintaining schema organization
- **`logs/`**: Execution logs and query results previews are saved here
- **`run_sql_script.py`**: The main script that orchestrates everything

## Setup Instructions

### Step 1: Create and Activate Virtual Environment

**For Windows:**
```bash
Goole it.
```

**For Mac/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `pandas`: For data manipulation and CSV handling
- `duckdb`: For in-memory SQL query execution

### Step 3: Run Your First Query

Execute one of the example queries:

```bash
python run_sql_script.py queries/sample_schema/cube_example.sql data/sample_schema/ logs/cube_log.txt
```

### Step 4: Check Results

After successful execution, you'll find:
- **Log file**: `logs/cube_log.txt` - Contains execution details and query results preview
- **CSV output**: `outputs/sample_schema/cube_example_output.csv` - Full query results in CSV format

## Usage Guide

### Basic Command Structure

```bash
python run_sql_script.py <path_to_sql_file> <path_to_data_folder> <path_to_log_file>
```

### Parameters Explained

1. **`<path_to_sql_file>`**: Path to your SQL query file (must end with `.sql`)
2. **`<path_to_data_folder>`**: Folder containing CSV files to load as tables
3. **`<path_to_log_file>`**: Where to save execution logs and results

### Example Commands

```bash
# Run the CUBE example
python run_sql_script.py queries/sample_schema/cube_example.sql data/sample_schema/ logs/cube_analysis.txt

# Run the JOIN example
python run_sql_script.py queries/sample_schema/join_example.sql data/sample_schema/ logs/join_analysis.txt

# Run the aggregation example
python run_sql_script.py queries/sample_schema/aggregation_example.sql data/sample_schema/ logs/agg_analysis.txt
```

## 📚 Working with New Assignments or Datasets

This section explains how students can set up the project for new assignments, different datasets, or their own projects.

### Step-by-Step Guide for New Assignments

#### Step 1: Create Your Data Schema Folder
Create a new subfolder inside `data/` for your specific assignment or dataset:

```bash
# Example: For Assignment 2
mkdir data/assignment2_schema

# Or for a specific project
mkdir data/retail_analysis_schema
mkdir data/healthcare_schema
```

#### Step 2: Add Your CSV Files
Place your CSV data files inside your new schema folder:

```
data/
└── assignment2_schema/          # Your new assignment folder
    ├── customers.csv            # Your dimension tables
    ├── products.csv
    ├── orders.csv               # Your fact tables
    └── order_items.csv
```

**Important**: 
- CSV files must have headers in the first row
- Table names in your SQL queries will match the CSV filename (without .csv extension)
- Example: `customers.csv` becomes table `customers` in your SQL

#### Step 3: Create Your Queries Folder
Create a matching folder in `queries/` with the **exact same name**:

```bash
# Must match your data folder name exactly
mkdir queries/assignment2_schema
```

#### Step 4: Write Your SQL Queries
Create your `.sql` files inside your queries schema folder:

```
queries/
└── assignment2_schema/          # Matches your data folder name
    ├── customer_analysis.sql    # Your custom queries
    ├── sales_report.sql
    └── monthly_trends.sql
```

**Example SQL query** (`queries/assignment2_schema/customer_analysis.sql`):
```sql
-- Customer Analysis for Assignment 2
SELECT 
    c.customer_name,
    COUNT(o.order_id) as total_orders,
    SUM(oi.quantity * oi.unit_price) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_spent DESC;
```

#### Step 5: Run Your Queries
Use the same command pattern with your new schema name:

```bash
# Run your custom query
python run_sql_script.py queries/assignment2_schema/customer_analysis.sql data/assignment2_schema/ logs/assignment2_results.txt
```

#### Step 6: Find Your Results
Your results will be automatically organized:

- **Log file**: `logs/assignment2_results.txt` (execution details + preview)
- **CSV output**: `outputs/assignment2_schema/customer_analysis_output.csv` (full results)

### Quick Reference Commands

```bash
# 1. Create folders
mkdir data/your_schema_name
mkdir queries/your_schema_name

# 2. Add your CSV files to data/your_schema_name/

# 3. Create SQL queries in queries/your_schema_name/

# 4. Run your analysis
python run_sql_script.py queries/your_schema_name/your_query.sql data/your_schema_name/ logs/your_log.txt
```

### Multiple Assignments Example

You can work on multiple assignments simultaneously:

```
data_warehouse_lab/
├── data/
│   ├── sample_schema/           # Provided examples
│   ├── assignment1_schema/      # Your Assignment 1
│   ├── assignment2_schema/      # Your Assignment 2
│   └── final_project_schema/    # Your final project
│
├── queries/
│   ├── sample_schema/           # Example queries
│   ├── assignment1_schema/      # Assignment 1 queries
│   ├── assignment2_schema/      # Assignment 2 queries
│   └── final_project_schema/    # Final project queries
│
└── outputs/
    ├── sample_schema/           # Example results
    ├── assignment1_schema/      # Assignment 1 results
    ├── assignment2_schema/      # Assignment 2 results
    └── final_project_schema/    # Final project results
```

### Tips for Success

1. **Consistent Naming**: Always use the same name for your data and queries folders
2. **Descriptive Names**: Use clear folder names like `assignment2_schema` or `retail_analysis`
3. **CSV Headers**: Ensure your CSV files have column headers in the first row
4. **SQL Table Names**: Reference tables using the CSV filename without the .csv extension
5. **Test Early**: Start with simple queries to verify your data loads correctly

## Sample Data Description

The project includes a sample retail dataset with three tables:

### 1. `dim_customer` (Customer Dimension)
- **customer_id**: Unique customer identifier
- **customer_name**: Customer's full name
- **city, region, country**: Geographic information
- **age_group**: Customer age demographic
- **segment**: Business segment (Consumer, Corporate, Home Office)

### 2. `dim_product` (Product Dimension)
- **product_id**: Unique product identifier
- **product_name**: Product name
- **category, subcategory**: Product classification
- **brand**: Product brand
- **unit_price, cost**: Pricing information

### 3. `fact_sales` (Sales Fact Table)
- **sale_id**: Unique transaction identifier
- **customer_id, product_id**: Foreign keys to dimension tables
- **sale_date**: Transaction date
- **quantity**: Number of items sold
- **total_amount**: Total transaction value
- **discount_percent**: Applied discount
- **sales_rep**: Sales representative
- **region**: Sales region

## Example Queries Explained

### 1. CUBE Example (`cube_example.sql`)
Demonstrates multi-dimensional analysis using the CUBE operator:
- Analyzes sales across region, category, and age group
- Shows subtotals and grand totals for all combinations
- Perfect for understanding OLAP cube concepts

### 2. JOIN Example (`join_example.sql`)
Shows how to combine fact and dimension tables:
- Joins all three tables for comprehensive reporting
- Calculates profit margins and other derived metrics
- Demonstrates typical data warehouse reporting patterns

### 3. Aggregation Example (`aggregation_example.sql`)
Focuses on various aggregation functions:
- COUNT, SUM, AVG, MIN, MAX operations
- GROUP BY with HAVING clauses
- Business metrics calculation

## Creating Your Own Queries

1. **Create a new SQL file** in the appropriate schema folder:
   ```sql
   -- queries/sample_schema/my_analysis.sql
   SELECT 
       c.region,
       COUNT(*) as total_orders,
       SUM(f.total_amount) as revenue
   FROM fact_sales f
   JOIN dim_customer c ON f.customer_id = c.customer_id
   GROUP BY c.region
   ORDER BY revenue DESC;
   ```

2. **Run your query**:
   ```bash
   python run_sql_script.py queries/sample_schema/my_analysis.sql data/sample_schema/ logs/my_results.txt
   ```

## Adding Your Own Data

1. **Add CSV files** to the `data/sample_schema/` folder (or create a new folder)
2. **Ensure proper CSV format** with headers in the first row
3. **Reference tables** in your SQL using the filename without extension
   - Example: `sales_data.csv` becomes table `sales_data`

## Troubleshooting

### Common Issues and Solutions

**Error: "SQL file not found"**
- Check that the path to your SQL file is correct
- Ensure the file has a `.sql` extension

**Error: "No CSV files found"**
- Verify the data folder path is correct
- Ensure CSV files are in the specified folder
- Check that files have `.csv` extension

**Error: "Query execution failed"**
- Check your SQL syntax
- Verify table names match CSV filenames (without extension)
- Review the log file for detailed error messages

**Error: "Failed to load CSV"**
- Ensure CSV files are properly formatted
- Check for encoding issues (should be UTF-8)
- Verify CSV headers are present

### Getting Help

1. **Check the log file** - It contains detailed error information
2. **Review SQL syntax** - Ensure proper DuckDB SQL syntax
3. **Verify table names** - Must match CSV filenames exactly
4. **Test with sample queries** - Start with provided examples

## Learning Exercises

### Beginner Level
1. Modify the aggregation example to analyze different dimensions
2. Create a simple query to find top-selling products
3. Write a query to analyze sales by month

### Intermediate Level
1. Create a query using window functions (ROW_NUMBER, RANK)
2. Implement a ROLLUP operation for hierarchical totals
3. Build a query to calculate year-over-year growth

### Advanced Level
1. Create complex CTEs (Common Table Expressions)
2. Implement advanced analytical functions
3. Build a comprehensive sales dashboard query

## Project Structure Best Practices

- Keep SQL files focused on specific analysis topics
- Use descriptive names for queries and log files
- Document your queries with comments
- Organize data into logical folders by subject area
- Regular cleanup of log files to manage disk space

---

**Happy Learning!** 🎓

This project is designed to give you hands-on experience with data warehouse concepts. Start with the example queries, then experiment with your own analyses to deepen your understanding of SQL and data warehousing principles.