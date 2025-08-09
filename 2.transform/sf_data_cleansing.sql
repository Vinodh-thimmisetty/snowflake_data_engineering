-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

use role sysadmin;
use database aqi_dev_db;
use schema clean;

with all_aqi_info as (
select 
    id,
    index_record_ts,
    json_data,
    json_data_records as record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from staging.RAW_AQI_DATA 
where index_record_ts is not null
),
unique_aqi_info as (
    select *
    from all_aqi_info
    where latest_file_rank = 1
)
select 
    index_record_ts,

     each_row.value:country::text as country,
     each_row.value:state::text as state,
     each_row.value:city::text as city,
     each_row.value:station::text as station,
     each_row.value:latitude::number(12,7) as latitude,
     each_row.value:longitude::number(12,7) as longitude,
     each_row.value:pollutant_id::text as pollutant_id,
     each_row.value:pollutant_max::text as pollutant_max,
     each_row.value:pollutant_min::text as pollutant_min,
     each_row.value:pollutant_avg::text as pollutant_avg,

     _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts,
from unique_aqi_info,
lateral flatten(input => json_data:records) each_row;

CREATE OR REPLACE DYNAMIC TABLE AQI_DEV_DB.CLEAN.CLEAN_AQI_DATA
TARGET_LAG = 'downstream'
WAREHOUSE = TRANSFORM_WH
AS
with unique_aqi_info as (
select 
    id,
    index_record_ts,
    json_data,
    json_data_records as record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
from staging.RAW_AQI_DATA 
where index_record_ts is not null
qualify row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) = 1
),
parsed_records as (
    select 
        index_record_ts,
        f.value as record,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    from unique_aqi_info,
    lateral flatten(input => json_data:records) f
)
select 
    index_record_ts,
    replace(record:country::text, '_', ' ') as country,
    replace(record:state::text, '_', ' ') as state,
    replace(record:city::text, '_', ' ') as city,
    replace(record:station::text, '_', ' ') as station,
    record:latitude::number(12,7) as latitude,
    record:longitude::number(12,7) as longitude,
    record:pollutant_id::text as pollutant_id,
    zeroifnull(try_cast(record:pollutant_max::text AS NUMBER)) as pollutant_max,
    zeroifnull(try_cast(record:pollutant_min::text AS NUMBER)) as pollutant_min,
    zeroifnull(try_cast(record:pollutant_avg::text AS NUMBER)) as pollutant_avg,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts
from parsed_records;


CREATE OR REPLACE DYNAMIC TABLE AQI_DEV_DB.CLEAN.CLEAN_FLATTEN_AQI_DATA
TARGET_LAG = 'downstream'
WAREHOUSE = TRANSFORM_WH
AS
SELECT
    index_record_ts,
    country,
    state,
    city,
    station,
    latitude,
    longitude, 
    ZEROIFNULL(MAX(IFF(pollutant_id = 'PM2.5', ROUND(pollutant_avg), NULL))) as pm25_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'PM10', ROUND(pollutant_avg), NULL))) as pm10_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'SO2', ROUND(pollutant_avg), NULL))) as so2_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'NO2', ROUND(pollutant_avg), NULL))) as no2_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'NH3', ROUND(pollutant_avg), NULL))) as nh3_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'CO', ROUND(pollutant_avg), NULL))) as co_avg,
    ZEROIFNULL(MAX(IFF(pollutant_id = 'OZONE', ROUND(pollutant_avg), NULL))) as o3_avg
FROM AQI_DEV_DB.CLEAN.CLEAN_AQI_DATA
GROUP BY ALL;


select * from AQI_DEV_DB.CLEAN.CLEAN_FLATTEN_AQI_DATA LIMIT 10;