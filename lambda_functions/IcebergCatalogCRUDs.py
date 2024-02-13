import boto3
import json
import operator
#one way to keep track of all the partitions and decide which one to delete is to have a partitions.json file in an s3 bucket, which represents source of truth for the number of partitions currently in the datalake, fetch it every time you want to update the iceberg table and compare the two lists to ouput three lists; toDelete, toAdd and toKeep


def lambda_handler(event, context):
    #get all the partitions from glue catalog
    client=boto3.client('glue')
    metadata=client.get_partitions(DatabaseName='raw_tickets',
        TableName='raw_sporting_event_cdc')
    allPartitions=[]
    #get the partitions in date format 
    for i, v in enumerate(metadata['Partitions']):
        allPartitions.append('-'.join(v['Values']))


    #ouput list that has all the cataloged partitions (glue does not automatically update the partitions so you would need to manually delete old partitions)
    allPartitions


    #get the latest version of the source of truth partitions file
    s3=boto3.client('s3')
    bucket='iceberg-cdc-metdata'
    objs=s3.list_objects(Bucket=bucket)
    for obj in objs.get("Contents"):
        data=s3.get_object(Bucket=bucket, Key=obj.get('Key'))
        SoTPartitions=json.loads(data['Body'].read().decode("utf-8"))['Partitions']

    

    s3=boto3.resource('s3')
    #get the older version of the SoT partitions right before updating the catalog
    bucket=s3.Bucket(
        'iceberg-cdc-metdata',
        )

    versions=sorted(bucket.object_versions.filter(
        Prefix='SoTPartitions'),
        key=operator.attrgetter("last_modified"),
        reverse=True)

    s3=boto3.client('s3')
    SoTPartitions_U0=s3.get_object( 
        Bucket='iceberg-cdc-metdata', 
        Key='SoTPartitions.json',
        #the older version right before updating that catalog will be the second index in the list
        VersionId=versions[1].id
        )

    SoTPartitions_U0=json.loads(SoTPartitions_U0['Body'].read().decode("utf-8"))['Partitions']
    SoTPartitions_U0

    toKeep=set(allPartitions).intersection(SoTPartitions) # qa rule to confirm number of files vs number of partitions to keep 
    toDelete=list(set(allPartitions) - set(SoTPartitions)) # pass on to glue.py 
    toAdd=list(set(SoTPartitions) -set(SoTPartitions_U0))
    #once you've deleted the partitions no longer there, then allPartitions becomes the new SoTPartitions file which you will upload to s3


    updateDict={
    'toDelete': 
    toDelete,

    'toAdd':
        toAdd
    }


    return updateDict