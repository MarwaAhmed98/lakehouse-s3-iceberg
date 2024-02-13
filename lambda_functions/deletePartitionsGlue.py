import boto3

def lambda_handler(event, context):
    
    client=boto3.client('glue')

    #get the list to delete from previous lambda
    toDeletePartitions= event['toDelete']

    if toDeletePartitions:
            
        pts=[v.split('-') for idx, v in enumerate(toDeletePartitions)]
    

        #flatten the dates 
        pts_flattened=[v for pt in pts for v in pt]


        response = client.batch_delete_partition(
            DatabaseName='raw_tickets',
            TableName='raw_sporting_event_cdc',
        PartitionsToDelete=[
                {
                    'Values': pts_flattened
                },
            ]

        )
    else:
        print("No partitions to delete...")

    return response
