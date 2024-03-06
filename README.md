Quality Movie Data Ingestion AWS Data Engineering Project
---------------------------------------------------------

Introduction
------------
The goal of this project is to perform data analytics on IMDb data using various tools and technologies, including AWS S3, Python, Redshift, Glue ETL.

Architecture
------------
![Project Architecture](https://github.com/xalxoelias/quality-movie-data-ingestion-aws-data-engineering-project/assets/87695923/ccbf3df6-9a81-4e1e-9289-1d89d87b7e1b)

Description
-----------
- Raw data ingestion from S3.
- Transformation utilizing AWS Glue ETL processes.
- Application of data quality business rules (e.g., ingesting data where rating > 8, checking completeness).
- Implementation of two separate crawlers for S3 and Redshift, storing metadata in the database for both.
- Evaluation of data quality; successful results are loaded into a data warehouse(Redshift) for analytical purposes.
- Failed data is stored in S3 for further analysis using Athena.
  
