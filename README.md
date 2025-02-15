# H-1B Data Analysis using PySpark & Docker

## Description
This project is designed to analyze **H-1B visa data** using **PySpark**, **Docker**, and **Power BI** while handling large datasets efficiently. It leverages **Apache Spark** for distributed data processing, **Docker** for environment management, and **PostgreSQL** for data warehousing.

The setup includes:
- A **Dockerized environment** with Spark, Python, and dependencies pre-configured.
- Efficient **ETL pipelines** that process and store data in a PostgreSQL data warehouse.
- A **mini data lake** using Parquet files to optimize storage and retrieval.
- A structured **data pipeline** controlled via threading for concurrent execution.
- **SQL scripts** to construct **SDE** and **SIL tables**, which serve as Power BI inputs.
- Processing **large datasets (2GB+ files)** efficiently with Spark.
- Python scripts written using **object-oriented programming (OOP)** principles to enhance modularity and reusability.
- The **SIL tables** are still in development, aiming to complete the data pipeline for analytical reporting.

To gain real-time experience with Spark, I aimed to set up this environment on a **Linux system** and successfully configure all necessary components. The project includes a **Docker setup** that automates the installation of Spark, Python, and necessary dependencies, while also enabling **file transfers between the local machine and the Docker container**. Additionally, the configuration includes a **network gateway** for accessing a PostgreSQL database installed on my Windows machine.

## Project Structure

### **Main Components**
- `.gitignore` - Excludes input files, logs, and compiled Python files.
- `README.md` - Documentation for the repository.

### **Code Directory (`code/`)**
Houses all Python scripts structured using **object-oriented programming (OOP)** principles for reusability.

#### **Core (`code/core/`)**
- `etl_job.py` - Defines ETL pipeline steps for ingesting and processing H-1B data using PySpark and Pandas.
- `spark_configuration.py` - Manages Spark settings (executors, driver memory, partitions, Kryo serialization, etc.).
- `spark_utilities.py` - Provides Spark-related utility functions for DataFrame operations and database writing.
- `threadexecutor.py` - Implements threading to process multiple tables concurrently, optimizing execution time.
- `trigger_pipeline.py` - Controls the execution and management of the ETL pipeline.

#### **Database (`code/database/`)**
- `postgresconnector.py` - PostgreSQL connector with query execution, row count retrieval, and thread-safe transactions.

#### **JSON Manipulations (`code/jsonmanipulations/`)**
- `jsonvalueextract.py` - Extracts deeply nested JSON values using key paths.
- `jsontagvariables.py` - Stores JSON key variables for structured reference.
- `configparametervalue.py` - Pre-loads configuration values from JSON for seamless access across the program.

#### **Root (`code/root/`)**
- `commonvariables.py` - Stores global variables used across the program.
- `populate_pipeline_main.py` - Main script triggering the data pipeline.

#### **Testing (`code/test/`)**
Contains Jupyter notebooks and scripts used for data exploration and debugging:
- `code-actual.ipynb`, `code-load_csv.ipynb`, `code.ipynb`, `code_first.py`
- `pyspark_test.py`, `sparkdatadownload.py`, `test.py`

#### **Utilities (`code/utility/`)**
- `pandas_utilities.py` - Helper functions for Pandas DataFrame operations.
- `utils.py` - General-purpose utility functions.

### **Configuration (`config/`)**
- `configurations.json` - Main configuration file.
- `database.config` - PostgreSQL connection settings.
- `pyspark_configurations.json` - Spark-specific configurations.
- `group_dwh_tables.json` - Contains the list of tables and their configurations for processing.
- `group_dwh_tables-tempruns.json`

### **Miscellaneous (`etc/`)**
- `json_split.sh` - Shell script used for initial development to partition a large (5GB) dataset for Spark processing.

### **Scripts (`scripts/`)**
- `apache_spark_container.dockerfile` - Dockerfile for setting up the Spark environment.
- `requirements.txt` - Lists Python package dependencies.
- `start-up.txt` - Startup instructions.

### **SQL (`sql/`)**
Contains SQL scripts for constructing and managing the target data warehouse:
- `analysis_scripts.sql`
- `data_correction_stg_loc_disclosure.sql`

## Docker Setup

The **Dockerfile** provisions an Ubuntu-based container with:
- **Java & Spark Installation** for PySpark processing.
- **SSH Configuration** for secure remote access.
- **Custom User (`root`)** with sudo privileges.
- **Port Exposures** for services such as Spark UI (8080, 4040) and PostgreSQL (5432).
- **Persistent Storage** to enable seamless file transfers between host and container.

### **Build and Run the Docker Container**
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

### **Running Spark Jobs Inside the Container**
To execute Spark jobs within the container, run the following commands, these are present in scripts,starupfile.txt:
```sh
# this accomadates all tables .
/home/john_user/pyspark_learning_venv/bin/python /home/john_user/pyspark_learning/code/root/populate_pipeline_main.py \
/home/john_user/pyspark_learning/config/configurations.json \
/home/john_user/pyspark_learning/config/pyspark_configurations.json \
/home/john_user/pyspark_learning/config/database.config \
/home/john_user/pyspark_learning/config/group_dwh_tables.json \
2> /root/docker-apachespark/execution_error_log.txt

# for temp runs 
/home/john_user/pyspark_learning_venv/bin/python /home/john_user/pyspark_learning/code/root/populate_pipeline_main.py \
/home/john_user/pyspark_learning/config/configurations.json \
/home/john_user/pyspark_learning/config/pyspark_configurations.json \
/home/john_user/pyspark_learning/config/database.config \
/home/john_user/pyspark_learning/config/group_dwh_tables-tempruns.json \
2> /root/docker-apachespark/execution_error_log.txt
```

## Development Workflow

This project follows a **branching workflow**:
- **Development (`dev` branch)** - All feature implementations and updates.
- **Main (`main` branch)** - Stable, production-ready code.

## Future Enhancements
- Automate Spark job scheduling with **Airflow**.
- Optimize performance for large dataset processing.
- Leadn Spark MLib and integrate utilities on my dataset .

