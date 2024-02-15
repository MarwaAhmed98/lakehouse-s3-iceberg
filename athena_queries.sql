create database raw_tickets;
drop  table raw_tickets.sporting_event;
--full data load
create external table raw_tickets.sporting_event(
  op string,
  cdc_timestamp timestamp, 
  id bigint, 
  sport_type_name string, 
  home_team_id int, 
  away_team_id int, 
  location_id smallint, 
  start_date_time timestamp, 
  start_date date, 
  sold_out smallint)
row format delimited 
  fields terminated by ',' 
stored as inputformat 'org.apache.hadoop.mapred.textinputformat' 
outputformat 'org.apache.hadoop.hive.ql.io.hiveignorekeytextoutputformat'
location 's3://iceberg-cdc/sporting_event_full/';


--check the data loaded correctly
select * from raw_tickets.sporting_event limit 5;

--cdc table

create external table raw_tickets.sporting_event_cdc(
op string,
cdc_timestamp timestamp,
id bigint,
sport_type_name string,
home_team_id int,
away_team_id int,
location_id smallint,
start_date_time timestamp,
start_date date,
sold_out smallint)
partitioned by (partition_date string)
row format delimited
fields terminated by ','
stored as inputformat 'org.apache.hadoop.mapred.textinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.hiveignorekeytextoutputformat'
location 's3://iceberg-cdc/sporting_event_cdc/2022/09/22/';

--add partition
alter table raw_tickets.sporting_event_cdc add partition (partition_date='2022-09-22') location 's3://iceberg-cdc-prod/topics/event_tickets.raw_tickets.sporting_event/year=2022/month=09/day=21/';

--check the table loaded properly 
select * from raw_tickets.sporting_event_cdc;

create database raw_tickets;

--create the iceberg source table
create table curated_tickets.sporting_event
with (table_type='iceberg',
location='s3://iceberg-cdc/curated/sporting_event',
format='parquet',
is_external=false)
as select
id,
sport_type_name,
home_team_id,
away_team_id,
cast(location_id as int) as location_id,
cast(start_date_time as timestamp(6)) as start_date_time,
cast(start_date as date) as start_date,
cast(sold_out as int) as sold_out
from raw_tickets.sporting_event;

--iceberg acid transactions
merge into curated_tickets.sporting_event t 
using (
select __op,
cdc_timestamp,
id,
sport_type_name,
home_team_id,
away_team_id,
location_id,
cast(from_iso8601_timestamp(start_date_time as timestamp) as start_date_time,
cast(cast(from_iso8601_timestamp(start_date_time as timestamp) as date) as start_date,
sold_out
from prod_event_tickets_raw_tickets_sporting_event
where year='2022' and month='09' and day='21') s on t.id = s.id
when matched and s.	__op='d' then delete
when matched then update set sport_type_name = s.sport_type_name,
home_team_id = s.home_team_id,
location_id = s.location_id,
start_date_time = s.start_date_time,
start_date = s.start_date,
sold_out = s.sold_out
when not matched and s.	__op='c' then insert (id,
sport_type_name,
home_team_id,
away_team_id,
location_id,
start_date_time,
start_date
)
values
(s.id,
s.sport_type_name,
s.home_team_id,
s.away_team_id,
s.location_id,
s.start_date_time,
s.start_date);


--check the operations worked, ticket id 21 should be deleted
select * from curated_tickets.sporting_event where id in (1, 5, 11, 21);


--time travel, create a view of the previous snapshot 
create view curated_tickets.view_sporting_event_previous_snapshot as
select id,
sport_type_name,
home_team_id,
away_team_id,
location_id,
cast(start_date_time as timestamp(3)) as start_date_time,
start_date,
sold_out
from curated_tickets.sporting_event
for timestamp as of current_timestamp + interval '-55' minute;


--if it worked, ticket id 21 should be returned
select * from curated_tickets.view_sporting_event_previous_snapshot where id = 21;


--note that in athena you can wrap the table names in back quotes but not double quotes 

--dropping old partitions
alter table prod_event_tickets_raw_tickets_sporting_event drop partition(year='2022', month='09', day='21') 

alter table prod_event_tickets_raw_tickets_sporting_event add partition(year='2022', month='09', day='21') 



alter table raw_tickets.`sporting_event_cdc/sporting_event_cdc` add partition(partition_date='2022-09-22')
location 's3://iceberg-cdc/sporting_event_cdc/2022/09/22/';
--common errors encountered 
--* cannot change partition name >>> index issue 
--* unpexted projected columns internal error >> issue in schema
--* hive partition mismatch >>> partitions will inherit from the parent not its own schema

-- note that in boto3 athena queries will fail but it will show that it did not 


delete from curated_tickets.sporting_event where try(cast(my_column as double)) is not null

--create a snapshot of the table before the updates
create view curated_tickets.sporting_event_previous_snapshot as
select id,
sport_type_name,
home_team_id,
away_team_id,
location_id,
cast(start_date_time as timestamp(3)) as start_date_time,
start_date,
sold_out
from curated_tickets.sporting_event
for timestamp as of current_timestamp + interval '-30' minute;



create external table `prodevent_tickets_raw_tickets_sporting_event`(
  `op` string comment '', 
  `cdc_timestamp` string comment '', 
  `id` bigint comment '', 
  `sport_type_name` string comment '', 
  `home_team_id` int comment '', 
  `away_team_id` int comment '', 
  `location_id` int comment '', 
  `start_date_time` timestamp comment '', 
  `start_date` date comment '', 
  `sold_out` int comment '', 
  `__deleted` string comment '', 
  `__op` string comment '', 
  `__source_ts_ms` bigint comment '', 
  `ticket_status` string comment '')
partitioned by ( 
  `year` string comment '', 
  `month` string comment '', 
  `day` string comment '')
row format serde 
  'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde' 
stored as inputformat 
  'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat' 
outputformat 
  'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location
  's3://iceberg-cdc-prod/topics/event_tickets.raw_tickets.sporting_event/'
tblproperties (
  'crawlerschemadeserializerversion'='1.0', 
  'crawlerschemaserializerversion'='1.0', 
  'updated_by_crawler'='tickets-cdc-data-prod', 
  'averagerecordsize'='372', 
  'classification'='parquet', 
  'compressiontype'='none', 
  'objectcount'='6', 
  'partition_filtering.enabled'='true', 
  'recordcount'='11', 
  'sizekey'='25790', 
  'typeofdata'='file')