# lakehouse-s3-iceberg
Project files for building an automated lakehouse architecture on S3 using Iceberg 

In the Lambda directory, there are the following functions:
   - deletePartitionsGlue.py will send to batch_delete_partition() endpoint to delete any partitions from files that were manually removed 

   - IcebergCatalogCRUDs.py compares the current partitions in Glue with the Source of Truth partitions JSON file and outputs which partitions to delete and which to add
   
   - UpdateIceberg.py will update the icerberg table in s3 by running an Athena query to merge the new data based on newly added partitions 
   
   - UpdateSoTFile.py will send a request to S3 to read all the files in an S3 bucket created for CDC data and create a JSON output with the most updated year/month/day partitions present in the bucket