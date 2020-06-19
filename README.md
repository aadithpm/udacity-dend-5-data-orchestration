# Data Orchestration for Sparkify



The goal of this project is to build a data pipeline with Airflow for Sparkify. We use these datasets:


* Data from the Million Songs dataset that consists of song and artist data

* Logs generated with this dataset and the event generator



 We create an Airflow DAG with custom operators to:
 
 1. Pull data from an external source
 2. Load into staging tables on Redshift
 3. Load data from these staging tables into fact and dimension tables
 4. Perform data quality checks on data

 The data is pulled from S3 buckets, transformed and loaded into a 4 node `dc2.large` Redshift cluster. An IAM user is defined for this whole process with permissions to read from S3 buckets and access Redshift clusters. In the interest of time, Udacity's workspace with Airflow was used for DAG configuration and execution. To execute locally, a good suggestion would be to use Docker images with Airflow and Celery set up. 



This documentation is split up into these following sections:



1. Table design, schemas and DAG

2. Data extraction and transformation

3. Explanation of files in the repository

4. Running the scripts



## 1. Table Design and Schemas



![ER Diagram](https://i.imgur.com/8zYZVo3.png)

![DAG](https://i.imgur.com/WonxGGv.png)

**DAG key:**

![DAG key](https://i.imgur.com/UEK3LfT.png)


## 2. Data extraction and transformation



For our PostgreSQL data modeling project, we processed each log and song data file separately, processing each row and loading it into the database. Here, we use COPY queries to access both log and song data stored in S3 buckets with the appropriate IAM role. We add our AWS credentials (Secret and access key) and a connection string for our Redshift cluster to Airflow's connections. You can do this through:

* Airflow UI: Admin -> Connections
* Adding connections through the Airflow CLI
* Storing connections as environment variables or in a Docker .env file

After loading into our staging tables, we use standard INSERT queries to add data to our fact and dimension tables after which we carry out a data quality check. We create four custom operators for these steps:


* `StageToRedshiftOperator`
* `LoadFactOperator` 
* `LoadDimensionOperator`
* `DataQualityOperator`

These operators are explained in the next section.


## 3. Explanation of files in the repository



* `create_tables.sql`: Collection of SQL queries for creating staging, dimension and fact tables on Redshift. This was run by SSHing to the cluster before running the DAG



* `dags`
   * `udac_example_dag.py`: Creation and definition of our DAG. Define schedule, retries and other parameters for tasks. Pass in tables and data sources for the various operators and build out the DAG with depandancies

* `plugins`
   * `helpers`
      * `sql_queries.py`: Collection of INSERT and COPY queries for our staging, dimension and fact tables, used by the custom operators
    
   * `operators`
      * `stage_redshift.py`: Custom operator for loading data into a target table on a Redshift cluster from a target S3 bucket

      * `load_fact.py`: INSERT-ing data from a data source to a target fact table with the choice to truncate

      * `load_dimension.py`: INSERT-ing data from a data source to a target dimension table

      * `data_quality.py`: Data quality checks like checking for empty table, incorrect data, etc.

* `dwh.ipynb`: Jupyter notebook that acts as a testing environment to check data integrity, AWS credentials, connection to our cluster and verify the correctness of our pipeline. Also contains analytics queries, which is the final (through Admin -> Connections in the UI or )goal of our pipeline



* `etl.py`: Run all `COPY` and `INSERT` queries defined in `sql_queries.py` to move data from S3 buckets -> staging tables -> fact & dimension tables



## 4. Running the scripts



To run the scripts:

* **For Udacity:** Start an Airflow server in the Udacity workspace and point to this folder so that it can recognise the defined DAG. You can try refreshing the workspace if your imports don't work

* **Locally:** Easiest option would be to pull a Docker image with Airflow and Celery pre-installed. You can choose to use Redis/RabbitMQ as a broker for Celery if needed. After you spin up a container, you can add this code and configure Airflow similar to the previous option

For both cases, you will need to add AWS credentials and a connection string to a Redshift cluster to Airflow.

----


