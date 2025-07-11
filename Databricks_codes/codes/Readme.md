# Data Processing & Governance Toolkit for Spark

This repository contains a comprehensive toolkit for data engineering on Spark, particularly within the Databricks environment. It is composed of two primary classes:

  * **`BaseDataProcessorSpark`**: A powerful, general-purpose class for data ingestion, processing, transformation, and performance optimization.
  * **`DataDictionaryBuilder`**: A specialized class for automating data governance tasks, including the creation, maintenance, and AI-powered enrichment of a data dictionary.

-----

## ✅ `BaseDataProcessorSpark`: The Data Processing Engine

This class provides a robust set of methods for processing data with Spark, including file reading, sanitization, data transformations, and performance optimization for large data volumes.

### File Reading & Ingestion

  * `read_data()`
      * Reads `.csv`, `.parquet`, and `.xlsx` files with options for skipping rows, headers, specific sheets, etc.
  * `fetch_and_append_data()`
      * Finds and concatenates files in a directory that contain a specific string in their name.
  * `list_files()` / `output_files()`
      * Lists files from the input or output directories.
  * `read_csv_with_schema()`
      * Reads CSV files using a manually defined schema for precision.
  * `profile_file_sample()`
      * Performs a partial read of a file for profiling and schema detection.

### ⚖️ Processing and Cleanup

  * `sanitize_column_names()`
      * Removes accents, spaces, and special characters from column names.
  * `drop_columns()`, `rename_columns()`, `reorganize_columns()`
      * Provides direct manipulation of DataFrame columns.
  * `convert_type()`, `change_data_types()`
      * Converts the data type of specified columns.
  * `drop_duplicates()`, `drop_null_rows()`, `drop_specific_row()`
      * Removes duplicate, null, or unwanted rows.
  * `tratar_colunas()`, `substituir_valores()`, `replace_values()`
      * Replaces values in columns based on rules or expressions.
  * `criar_coluna()`
      * Creates a new column with a default or dynamic value.
  * `preencher_valores_nulos()`
      * Fills nulls with specified default values on a per-column basis.
  * `fill_nan_based_on_dtype()`
      * Fills null values based on the column's data type (e.g., 0 for numeric, empty string for text).
  * `resetar_indice()`
      * Adds an index column to the DataFrame (note: does not reset like in Pandas).

### 📈 Transformations and Views

  * `create_temp_view()`
      * Creates a temporary view of the DataFrame in Spark SQL.
  * `get_sql_query()`
      * Executes a SQL query within the current Spark context.
  * `pivot_or_unpivot()`
      * Performs `pivot` (transforming rows into columns) or `unpivot` (melt).
  * `combinar_dataframes()`
      * Joins two DataFrames based on a common column.
  * `ordenar_dataframe()`
      * Sorts a DataFrame by specified columns with ascending/descending control.
  * `selecionar_colunas()`
      * Selects a subset of desired columns.
  * `apply_function_to_dataframes()`
      * Applies a given function to a list of DataFrames.

### 🌐 Advanced Input/Output

  * `convert_to_parquet()`
      * Converts `.csv` or `.xlsx` files to `.parquet` for significantly better performance.
  * `write_partitioned_data()`
      * Saves a DataFrame partitioned by columns (e.g., by `year`, `state`).
  * `export_to_db()`
      * Exports a DataFrame to a Delta table via the Spark SQL Catalog.
  * `parallel_read_csv()`
      * Reads multiple CSV files in parallel using `multiprocessing` + Pandas for specific scenarios.
  * `decompress_files()`
      * Decompresses `.zip`, `.tar.gz`, `.gz` files.

### ⚡ Performance Optimization

  * `cache_dataframe()`
      * Applies `.cache()` to the DataFrame and materializes it to speed up subsequent actions.
  * `checkpoint_dataframe()`
      * Uses an intermediate checkpoint to persist the DataFrame's state, breaking the lineage.
  * `estimate_file_access_times()`
      * Estimates file read times based on type, size, CPU, and RAM.
  * `preprocess_data_file()`
      * Sanitizes column names of corrupted files before attempting to read them into Spark.

-----

## 🤖 `DataDictionaryBuilder`: The Automated Governance Service

This class automates the creation and maintenance of a data dictionary. It acts as a complete data governance service, capable of generating its own metadata using Databricks' built-in AI functions and keeping the central governance table perfectly synchronized.

### Infrastructure & Setup

  * `__init__()`
      * Initializes the builder with the target catalog, schema, table, and an optional path to a buffer file for ingesting externally generated metadata.
  * `create_governance_infrastructure()`
      * Ensures the target schema and the data dictionary Delta table exist. It can create the table from a default schema or infer it from the buffer file's header. It also adds necessary columns like `source_notebook_url` if they are missing.

### AI-Powered Governance Logic

  * `_find_available_ai_model()`
      * An internal helper method that programmatically discovers an available Databricks Foundation Model endpoint. It tests a prioritized list of models (e.g., DBRX, Llama3) and returns the first one that responds, making the solution resilient and environment-agnostic.
  * `generate_and_apply_ai_descriptions()`
      * The core AI-powered feature. It first discovers a working model, then identifies all tables and columns in the target schema that are missing a description. It calls the `ai_query()` function with tailored prompts to generate and apply high-quality comments directly to the table and column metadata within Databricks.
  * `fetch_table_descriptions()`
      * The synchronization engine. It reads the official comments from the Databricks `information_schema` (including those just generated by the AI) and intelligently updates the central data dictionary table. It uses a case-insensitive `MERGE` operation to replicate a single table's description across all rows belonging to that table, and it also populates the `source_notebook_url` to provide clear lineage.

### Data Ingestion & Helpers

  * `load_buffer_to_governance_table()`
      * Processes the optional text/CSV buffer file. It appends new metadata records to the governance table and, upon successful ingestion, clears the buffer file (leaving only the header) to prevent reprocessing.
  * `drop_duplicate_rows()`
      * A utility method to ensure data integrity by removing duplicate metadata entries from the governance table based on a composite key (catalog, schema, table, column).

-----

### 💡 Example Workflow: Running the Governance Service

The following shows how to orchestrate the `DataDictionaryBuilder` to run as a complete, automated service, for example within a Databricks Job.

```python
# --- HOW TO USE THE CLASS ---
if __name__ == '__main__':
    # Environment Configurations
    CATALOG = "your_catalog_name"
    SCHEMA = "your_schema_name"
    GOVERNANCE_TABLE = "your_table_name"

    # 1. Instantiate the builder
    builder = DataDictionaryBuilder(
        target_catalog=CATALOG,
        target_schema=SCHEMA,
        target_table=GOVERNANCE_TABLE
    )

    # 2. Ensure the governance table infrastructure exists
    builder.create_governance_infrastructure()
    
    # 3. (Optional) Load any new metadata from an external buffer file
    # builder.load_buffer_to_governance_table()

    # 4. Automatically generate descriptions with AI for any undocumented assets
    # This calls the internal Databricks API (`ai_query`) to enrich the metadata
    builder.generate_and_apply_ai_descriptions()

    # 5. Synchronize all descriptions (including the newly generated ones)
    # into the central governance table for reporting and analysis.
    builder.fetch_table_descriptions()

    print("\n--- Data Governance Process Completed Successfully ---")

```