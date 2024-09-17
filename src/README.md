# Data Engineering Exercise Submission

## Overview
**Architecture Overview**:
   - This system uses a python script (`extract.py`) to extract and transform data from the Open Library API.
   - A second python script (`load.py`) will pickup the processed files and load them into a database.
   - A mocked data model of the database is included in `data_model.png`
   - SQL queries can be found in `/src/queries.sql`
   - Recommended technologies:
     - `extract.py`: AWS Lambda, AWS Batch, or AWS Glue
     - `load.py`: AWS Batch or AWS Glue
     - Storage: AWS S3
     - Data warehousing: AWS Redshift
     - Orchestration: Apache Airflow (on AWS)

**Overall Assumptions**:
   - A low volume of data is extracted from the API.
   - The full dataset extracted from the API can fit in a relatively small machine's memory.
   - The data from the API is slow-changing.
   - The data is extracted daily to refresh the database tables.

**Setup and Execution**:
   - To run this demo, create a virtual env and execute the following commands from the root folder (this was developed with python 3.12.1):
     - `pip3 install -r ./requirements.txt`
     - `python3 ./extract.py`
       - Output files located in `/src/data/processed/`
     - `python3 ./load.py`
       - Output files located in `/src/data/authors/` and `/src/data/books/`

**Data Querying**:
   - Queries can be found in `/src/queries.sql` and contain queries for the following:
     - The number of books written each year by an author.
     - The average number of books written by an author per year.
     - Books published by year and total books published by each author.
   - To optimize these queries for larger data sets, we could consider these options (for Redshift):
     - Add a DISTKEY on authors.id and books.author_id to colocate common values
     - Create an OLAP Rollup table (or materialized view) with aggregated metrics precomputed for authors and books
     - Depending on the fields involved and volume of data in each table, a partitioning strategy could be selected based on author name, book publication date, or other.
   
**General Comments for Production Deployment**:
   - Decouple the extract and transform processes for scalability.
   - Consider two changes for loading the data in place of daily full table refreshes:
     - Individual inserts of new records only, which may save on unneccesary compute costs for redundant transform and loading.
     - Refresh the tables less often (weekly or less if very slowly changing data).
   - Consider implementing this full process as an Airflow DAG (on AWS).
   - Extract:
     - With a large dataset, look into ways to limit the data extracted each day to only recent data.
     - Consider how the pipeline will be scheduled and triggered (if not using Airflow).
   - Transform:
     - With a large dataset, consider implementing this in PySpark on AWS Glue.
     - If not using a full data refresh, move the surrogate key generation to the `load.py` job in order to accomodate existing surrogate keys and assign new ones.
   - Load:
     - Consider how reprocessing and data backfills may be achieved with low overhead.
     - If continuing to use a full table refresh, move the generation of author and book csv files to the transform job and use the load job to create tables directly from the files (as opposed to record by record insert).

**IaC and Security**:
   - IaC
     - IaC would be managed with Terraform. 
     - An example implementation would include these configurations:
       - Service accounts and IAM roles/permissions for each AWS service
       - Service-specific environment variables
       - AWS S3 Buckets, access levels, lifecycle policies, and bucket notifications
       - Redshift warehouse configuration
   - Security considerations:
     - Within Terraform, specific service account level permissions and roles would need to be set for each AWS service (principle of least privilege basis).
     - Any secret keys or encryption keys could be stored in AWS Secrets Manager, and pulled into specific jobs as needed.
