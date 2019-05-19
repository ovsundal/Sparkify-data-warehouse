# Sparkify-data-modelling

This is project 3/5 of Udacitys Data Engineering Nanodegree. Project goals are extracting data from an external AWS bucket,
 inserting this into staging tables, before it is finally loaded into a Redshift data warehouse.


#### Starting the program
1. Add all necessary configuration parameters in the dwh.cfg file
2. Execute "setup_redshift_cluster". This will create a AWS Redshift data warehouse
3. Once the cluster is ready, execute "create_tables.py". This will create all necessary DWH tables
4. Run "etl.py". This will copy data from the external S3 bucket into staging_tables, adapt the data, 
and then insert it into the DWH tables
5. To teardown the cluster and DWH, execute "teardown_redshift_cluster.py"
