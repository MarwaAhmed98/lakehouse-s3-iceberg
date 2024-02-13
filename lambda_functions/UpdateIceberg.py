import boto3 


def lambda_handler(event, context):
    athena=boto3.client('athena')

    toAddPartitions= event['toAdd']

    if toAddPartitions:

        for idx, v in enumerate(toAddPartitions):
            query_string=f"""merge into curated_tickets.sporting_event t
            using (
            select op,
            cdc_timestamp,
            id,
            sport_type_name,
            home_team_id,
            away_team_id,
            location_id,
            start_date_time,
            start_date,
            sold_out
            from raw_tickets.raw_sporting_event_cdc 
            where year={int(v.split('-')[0])} and month={int(v.split('-')[1])} and day={int(v.split('-')[2])}) s on t.id = s.id
            when matched and s.op='D' then delete
            when matched then update set sport_type_name = s.sport_type_name,
            home_team_id = s.home_team_id,
            location_id = s.location_id,
            start_date_time = s.start_date_time,
            start_date = s.start_date,
            sold_out = s.sold_out
            when not matched then insert (id,
            sport_type_name,
            home_team_id,
            away_team_id,
            location_id,
            start_date_time,
            start_date)

            VALUES
            (s.id,
            s.sport_type_name,
            s.home_team_id,
            s.away_team_id,
            s.location_id,
            s.start_date_time,
            s.start_date);
            """
            response = athena.start_query_execution(
                QueryString = query_string,
                QueryExecutionContext = {
                    'Database': 'curated_tickets'
                }, 
                ResultConfiguration = { 'OutputLocation': 's3://iceberg-cdc-athena-results/'}
            )
    else:
        print("No partitions to add...")
    return None