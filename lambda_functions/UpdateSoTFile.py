import boto3 
import json 


def lambda_handler(event, context):
    #initiate s3 client 
    s3=boto3.client('s3')
    
    bucket='iceberg-cdc'
    prefix='sporting_event_cdc'
    
    partitionNames=[]

    #list all objects in the main bucket and filter to retrieve the files only
    objs=s3.list_objects(Bucket=bucket, Prefix=prefix)
    
    for obj in objs.get("Contents"):
        if obj['Key'][-1]=='/':
            continue
        else:
            partitionNames.append(obj['Key'].split('.')[0].split('-')[0].split('/')[4])
    #get the data from the filename
    partitionNames=['-'.join([str(partitionNames[idx])[0:4],str(partitionNames[idx])[4:6],str(partitionNames[idx])[6:8]]) for idx in range(len(partitionNames))]
    
    #convert to a JSON dict
    SoTPartitions={
        "Partitions": 
        partitionNames
    }
    toUploadtoS3=json.loads(json.dumps(SoTPartitions))#json.dumps() converts dict to string and then to use the json(dict) object use json.loads()
    
    print(f'The length of Source of Truth file is {len(partitionNames)}')

    response=s3.put_object(
     Body=json.dumps(toUploadtoS3),
    Bucket='iceberg-cdc-metdata', 
    Key='SoTPartitions.json'
    )
    
    print(response)
    
    return None