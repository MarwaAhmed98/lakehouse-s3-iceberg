import boto3 
import json 


def lambda_handler(event, context):
    #initiate s3 client 
    s3=boto3.client('s3')
    
    bucket='iceberg-cdc'
    
    partitionNames=[]

    #list all objects in the main bucket and filter to retrieve the files only
    objs=s3.list_objects(Bucket=bucket)
    
    for obj in objs.get("Contents"):
        if obj['Key'].endswith('.parquet'):
            partitionNames.append(obj['Key'].split('=')[1].split('/')[0]+obj['Key'].split('=')[2].split('/')[0]+obj['Key'].split('=')[3].split('/')[0])
    #get unique dates only
    partitionNames=set(partitionNames)   


    partitionNames=list(partitionNames)

    #convert date strings to dates
    partitionNames=['-'.join([str(partitionNames[idx])[0:4],str(partitionNames[idx])[4:6],str(partitionNames[idx])[6:8]]) for idx in range(len(partitionNames))]

    print(f'Total length of partitions are {len(partitionNames)}')
    
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