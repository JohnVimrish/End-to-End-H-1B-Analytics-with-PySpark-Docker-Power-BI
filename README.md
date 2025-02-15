# H-1B Data Analysis using PySpark & Docker

## Description

This project focuses on learning and utilizing **PySpark**, **Docker**, and **Power BI** to analyze H-1B visa data. The goal is to process large datasets efficiently using **Apache Spark**, containerize the environment with **Docker**, and visualize insights through **Power BI**.

To gain real-time experience with Spark, I aimed to set up this environment on a **Linux system** and successfully configure all necessary components. The project includes a **Docker setup** that automates the installation of Spark, Python, and necessary dependencies, while also enabling **file transfers between the local machine and the Docker container**. Additionally, the configuration includes a **network gateway** for accessing a PostgreSQL database installed on my Windows machine.

This project handles **large datasets (2GB+ files)**, making efficient data processing and transfer critical.

## Project Structure

The project consists of the following components:

### **Main Directories & Files**

- \`\` - Excludes input files, logs, and compiled Python files.
- \`\` - Documentation for the repository.

### **Code Folder (**\`\`**)**

Contains all Python scripts for data processing and ETL operations.

#### **Core (**\`\`**)**

- `etl_job.py` - Defines the ETL pipeline steps for ingesting and processing H-1B data using PySpark and Pandas.
- `spark_configuration.py` - Configures Spark settings like executors, driver memory, shuffle partitions, and Kryo serialization.
- `spark_utilities.py` - Utility functions for Spark operations such as DataFrame transformations, filtering, cleaning, and database writing.
- `threadexecutor.py` - Implements multithreading to parallelize table processing for efficiency.
- `trigger_pipeline.py` - Controls and manages the execution of the ETL pipeline.

#### **Database (**\`\`**)**

- `postgresconnector.py` - PostgreSQL connector with methods for executing queries, retrieving row counts, and managing transactions with thread safety.

#### **JSON Manipulations (**\`\`**)**

- `jsonvalueextract.py` - Extracts values from deeply nested JSON structures.
- `jsontagvariables.py` - Defines JSON keys for structured reference.
- `configparametervalue.py` - Reads configuration values from JSON and initializes them for use across the program.

#### **Root (**\`\`**)**

- `commonvariables.py` - Defines common variables used throughout the project.
- `populate_pipeline_main.py` - The main script that triggers the data pipeline.

#### **Testing (**\`\`**)**

Contains Jupyter notebooks and Python scripts for data exploration and debugging:

- `code-actual.ipynb`
- `code-load_csv.ipynb`
- `code.ipynb`
- `code_first.py`
- `pyspark_test.py`
- `sparkdatadownload.py`
- `test.py`

These files were created for initial development, allowing experimentation with processing data from input files to the data warehouse using Spark. Additionally, a mini data lake was set up on a Windows machine to understand Parquet file storage and usage.

#### **Utilities (**\`\`**)**

- `pandas_utilities.py` - Utility functions for handling Pandas DataFrames.
- `utils.py` - General helper functions.

### **Configuration Folder (**\`\`**)**

Contains configuration files:

- `configurations.json` - Main configuration settings.
- `database.config` - Database connection parameters.
- `pyspark_configurations.json` - PySpark-specific configurations.
- `group_dwh_tables.json` - Defines database warehouse tables and their configurations.
- `group_dwh_tables-tempruns.json`

### **Miscellaneous (**\`\`**)**

- `json_split.sh` - A shell script used in initial development to split a 5GB file into partitions for Spark processing and conversion into Parquet files.

### **Scripts Folder (**\`\`**)**

- `apache_spark_container.dockerfile` - Defines the Docker container setup.
- `requirements.txt` - Lists required Python packages.
- `start-up.txt` - Startup instructions.

### **SQL Folder (**\`\`**)**

This folder contains SQL scripts used to create and manage the target data warehouse. It includes constructing **SDE** and **SIL** tables from the processed inputs. These **SIL tables** serve as the input for Power BI reports.

- `analysis_scripts.sql`
- `data_correction_stg_loc_disclosure.sql`

## Docker Setup

The **Dockerfile** provisions an Ubuntu-based container with:

- **Java & Spark Installation**: Required for PySpark processing.
- **SSH Configuration**: Enables secure remote access.
- \*\*Custom User \*\*\`\`: With sudo privileges for better control.
- **Port Exposures**: Enables access to services like Spark UI (8080, 4040), PostgreSQL (5432), and others.
- **Persistent Storage**: Allows seamless file transfers between host and container.

### Build and Run the Docker Container

1. Clone the repository:
   ```sh
   git clone https://github.com/JohnVimrish/End-to-End-H-1B-Analytics-with-PySpark-Docker-Power-BI.git
   ```
2. Navigate to the project directory:
   ```sh
   cd End-to-End-H-1B-Analytics-with-PySpark-Docker-Power-BI
   ```
3. Build and run the container:
   ```sh
   docker-compose -f scripts/apache_spark_container.dockerfile up --build
   ```

### Running Spark Jobs Inside the Container

Once inside the container, run PySpark scripts as follows:

```sh
spark-submit scripts/data_analysis.py
```

## Development Workflow

This project follows a **branching workflow**:

- **Development (**\`\`\*\* branch)\*\*: All changes are first pushed to `dev`.
- **Main (**\`\`\*\* branch)\*\*: Once tested, changes from `dev` are merged into `main`.

## Future Enhancements

- Implement real-time data streaming using Kafka.
- Automate Spark job scheduling with Airflow.
- Optimize performance for large dataset processing.

