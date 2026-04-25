Local HDFS Spark Data Pipeline
==============================

This project demonstrates a localized **Big Data Pipeline** using **Apache Spark**. It follows the Medallion Architecture (Bronze-Silver-Gold) to process, clean, and store data in optimized formats like Parquet with Snappy compression.

🚀 Overview
-----------

The goal of this pipeline is to simulate a production-grade HDFS data workflow on a local environment. It automates the ingestion of raw data and transforms it into a queryable "Silver" layer, ensuring data schema consistency and storage efficiency.

🛠️ Tech Stack
--------------

*   **Language:** Python
    
*   **Processing Engine:** Apache Spark (PySpark)
    
*   **Storage Format:** Apache Parquet (Snappy Compressed)
    
*   **Architecture:** Medallion Data Architecture (Bronze & Silver layers)
    

📁 Project Structure
--------------------
### 📁 Project Structure

```text
local_hdfs_spark_data_pipeline/
├── bigdata_pipeline.py      # Main entry point for the pipeline
├── bronze_to_silver.py      # Transformation logic: Raw -> Cleaned
├── spark-warehouse/         # Local metadata and table storage
│   └── sil_table/           # Parquet-backed Silver layer data
│       ├── _SUCCESS         # Job completion marker
│       └── part-0000x...    # Distributed Parquet data shards
└── README.md
```

⚙️ Pipeline Workflow
--------------------

1.  **Ingestion (Bronze):** Raw data is loaded into the pipeline.
    
2.  **Transformation (Silver):** \* Executed via [bronze\_to\_silver.py](https://www.google.com/search?q=https://github.com/NaveenKShijo/local_hdfs_spark_data_pipeline/blob/main/bronze_to_silver.py).
    
    *   Data is cleaned, types are cast, and duplicates are removed.
        
3.  **Storage:** The processed data is written to the spark-warehouse in **Snappy-compressed Parquet** format for high-performance downstream analytics.
    

🔧 Setup & Installation
-----------------------

### Prerequisites

*   [Java 8 or 11](https://www.oracle.com/java/technologies/downloads/) (Required for HDFS)

*   [Java 17](https://www.oracle.com/java/technologies/downloads/) (Required for Spark)
    
*   [Apache Spark](https://spark.apache.org/downloads.html) (Local installation)
    
*   [Python 3.x](https://www.python.org/)
    


📊 Future Enhancements
----------------------
    
*   \[ \] Integration with **Delta Lake** for ACID transactions.
    
*   \[ \] Automated unit testing for Spark transformations.
    

**Author:** [Naveen K Shijo](https://github.com/NaveenKShijo)
