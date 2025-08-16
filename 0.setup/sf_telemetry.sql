-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

ALTER ACCOUNT SET EVENT_TABLE = NONE;

show parameters like 'event_table' in account;

----- 1. LOGGING -----

use role sysadmin;
use database aqi_dev_db;
create or replace schema telemetry with managed access;

create or replace warehouse TELEMETRY_WH like COMPUTE_WH;

-- Follow : https://opentelemetry.io/
create or replace event table log_events;

show event tables;

desc event table log_events;

-- alter table log_events set change_tracking=true;
-- alter table log_events unset change_tracking;

-- alter account set event_table = aqi_dev_db.telemetry.log_events;
-- show parameters like 'event_table' in account;

-- Procudure Code Logging
create or replace procedure divide_two_numbers_sp(input1 number, input2 number)
returns number
language SQL
AS
DECLARE
    result_val number default 0;
BEGIN
    SYSTEM$LOG('INFO', 'Inputs:' || input1 || ' and ' || input2);
    IF (input2=0) THEN
        SYSTEM$LOG_ERROR('Denominator must be Non-Zero');
        RETURN NULL;
    ELSE
        SYSTEM$LOG_TRACE('Everything is Good !!');
        result_val := (input1/input2);
    END IF;
    SYSTEM$LOG('TRACE', 'Output : ' || result_val);
    return result_val;
END;

desc procedure divide_two_numbers_sp(number, number);
show procedures like 'divide_two_numbers_sp';
alter procedure divide_two_numbers_sp(number, number) set log_level=TRACE;

call divide_two_numbers_sp(18, 0);
call divide_two_numbers_sp(-18, 0);
call divide_two_numbers_sp(11.0, 0);
call divide_two_numbers_sp(-1.0, 0);
call divide_two_numbers_sp(18, 3);

select * from log_events;

/*
RESOURCE Attributes for Stored Procedure:

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 1985,
  "snow.executable.name": "DIVIDE_TWO_NUMBERS_SP(INPUT1 NUMBER, INPUT2 NUMBER):NUMBER(38,0)",
  "snow.executable.type": "PROCEDURE",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be6101-0003-5ac2-0009-005e000b8936",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517657722,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "telemetry.sdk.language": "sql"
}

*/

-- Anonymous Block LOGGING

select current_session();
ALTER SESSION SET LOG_LEVEL=TRACE;
DECLARE
    result_val number default 0;
    input1 number := 10;
    input2 number := 20;
BEGIN
    SYSTEM$LOG('INFO', 'Inputs:' || input1 || ' and ' || input2);
    IF (input2=0) THEN
        SYSTEM$LOG_ERROR('Denominator must be Non-Zero');
        RETURN NULL;
    ELSE
        SYSTEM$LOG_INFO('Everything is Good !!');
        result_val := (input1/input2);
    END IF;
    SYSTEM$LOG('DEBUG', 'Output : ' || result_val);
    return result_val;
END;

/*
RESOURCE Attributes for Standard Anonymous Block:
{
  "db.user": "VINODH",
  "snow.executable.type": "STATEMENT",
  "snow.query.id": "01be613a-0003-59ac-0009-005e000c003a",
  "snow.session.id": 2533678517657722,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "telemetry.sdk.language": "sql"
}
*/



create or replace procedure py_add_two_numbers_sp(num1 int, num2 int)
returns int
language python
packages = ('snowflake-snowpark-python')
runtime_version = 3.10
handler = 'add_inputs'
AS $$
import logging
logger  = logging.getLogger("Py Logger")

def add_inputs(session, i1: int, i2: int):
    logger.info(f"Inputs: {str(i1)} {str(i2)}")
    logger.warn(f"Inputs: {str(i1)} {str(i2)}")
    logger.debug(f"Inputs: {str(i1)} {str(i2)}")
    return_val = i1 + i2
    logger.error(f"Result : {return_val}")
    logger.fatal(f"Result : {return_val}")
    # logger.trace(f"Result : {return_val}")
    return return_val

$$;

alter procedure py_add_two_numbers_sp(int, int) set log_level= TRACE;

call py_add_two_numbers_sp(10, 20);
call py_add_two_numbers_sp(-10, -20);
call py_add_two_numbers_sp(10, -20);
call py_add_two_numbers_sp(10, 0);
call py_add_two_numbers_sp(0,10);


/*

RESOURCE Attributes for Python Procedure:

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 1923,
  "snow.executable.name": "PY_ADD_TWO_NUMBERS_SP(NUM1 NUMBER, NUM2 NUMBER):NUMBER(38,0)",
  "snow.executable.runtime.version": "3.10",
  "snow.executable.type": "PROCEDURE",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be6356-0003-5ac2-0009-005e000b8d6e",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517649562,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "telemetry.sdk.language": "python"
}

*/

create or replace procedure js_add_two_numbers_sp(num1 float, num2 float)
returns float 
language javascript
AS 
$$
    snowflake.log("trace", "<jscript> javascript sp");
    snowflake.log("info", "<jscript> javascript sp", NUM1);
    snowflake.log("debug", "<jscript> javascript sp", NUM2);
    snowflake.log("warn", "<jscript> javascript sp");
    snowflake.log("error", "<jscript> javascript sp");
    snowflake.log("fatal", "<jscript> javascript sp");

    return (NUM1+NUM2);
$$;

alter procedure js_add_two_numbers_sp(float, float) set log_level=INFO;


call js_add_two_numbers_sp(10.0, 20.1);
call js_add_two_numbers_sp(-10, -20);
call js_add_two_numbers_sp(10, -20);
call js_add_two_numbers_sp(10, 0);
call js_add_two_numbers_sp(0,10);


/*
RESOURCE Attributes for JS Procedure:

**DANGER* lowercase v/s uppercase input variable Names issue ?

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 2114,
  "snow.executable.name": "JS_ADD_TWO_NUMBERS_SP(NUM1 FLOAT, NUM2 FLOAT):FLOAT",
  "snow.executable.type": "PROCEDURE",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be6360-0003-59de-0009-005e000c133a",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517649562,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "telemetry.sdk.language": "javascript"
}

*/


create or replace procedure java_logging_tutorial()
returns varchar
language java
runtime_version = '11'
packages = ('com.snowflake:telemetry:latest', 'com.snowflake:snowpark:latest')
handler = 'JavaLoggingHandler.runLogger'
AS 
$$

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.snowflake.snowpark_java.Session;

public class JavaLoggingHandler {
    private static final Logger log = LoggerFactory.getLogger(JavaLoggingHandler.class);
    public String runLogger(Session session){
        log.atInfo().addKeyValue("custom1", "Value1").setMessage("Logging with attributes").log();
        log.atDebug().setMessage("Debugging Level").log();
        log.atError().setMessage("Error Level").log();
        return "SUCCESS !!"; 
    } 
}
$$;

alter procedure java_logging_tutorial() set log_level=TRACE;

call java_logging_tutorial();


/*
RESOURCE Attributes for JAVA Procedure:

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 1859,
  "snow.executable.name": "JAVA_LOGGING_TUTORIAL():VARCHAR",
  "snow.executable.runtime.version": "11",
  "snow.executable.type": "PROCEDURE",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be636a-0003-59ac-0009-005e000c0412",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517649562,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "snow.worker.id": 0,
  "telemetry.sdk.language": "java"
}
*/


create or replace procedure scala_logging_tutorial()
returns varchar
language SCALA
runtime_version = '2.12'
packages = ('com.snowflake:telemetry:latest', 'com.snowflake:snowpark:latest')
handler = 'ScalaLoggingHandler.runLogger'
AS 
$$

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.snowflake.snowpark.Session;

class ScalaLoggingHandler {
    private val log: Logger = LoggerFactory.getLogger(getClass);
    def runLogger(session: Session):String = {
        log.atInfo().addKeyValue("custom1", "Value1").setMessage("Logging with attributes").log();
        log.atDebug().setMessage("Debugging Level").log();
        log.atError().setMessage("Error Level").log();
        return "SUCCESS !!"; 
    } 
}
$$;

alter procedure scala_logging_tutorial() set log_level=TRACE;

call scala_logging_tutorial();

/*

RESOURCE Attributes for SCALA Procedure:

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 1820,
  "snow.executable.name": "SCALA_LOGGING_TUTORIAL():VARCHAR",
  "snow.executable.runtime.version": "2.12",
  "snow.executable.type": "PROCEDURE",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be636e-0003-5bcf-0009-005e000c4336",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517649562,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "snow.worker.id": 0,
  "telemetry.sdk.language": "scala"
}
*/


create or replace function is_positive_int(i int)
returns BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
handler = 'check_positive'
AS 
$$
import logging

logger = logging.getLogger("python function_logger")

def check_positive(i):
    logger.info("INFO level for UDF")
    # logger.trace("TRACE level for UDF")
    logger.debug("DEBUG level for UDF")
    logger.error("ERROR level for UDF")
    logger.fatal("FATAL level for UDF")
    return (i>0);

$$;

alter function is_positive_int(int) set log_level=TRACE;

select is_positive_int(-10), is_positive_int(10);

/*
RESOURCE Attributes for FUNCTION

{
  "db.user": "VINODH",
  "snow.database.id": 22,
  "snow.database.name": "AQI_DEV_DB",
  "snow.executable.id": 1927,
  "snow.executable.name": "IS_POSITIVE_INT(I NUMBER):BOOLEAN",
  "snow.executable.runtime.version": "3.10",
  "snow.executable.type": "FUNCTION",
  "snow.owner.id": 4,
  "snow.owner.name": "SYSADMIN",
  "snow.query.id": "01be6375-0003-59ac-0009-005e000c04a2",
  "snow.schema.id": 95,
  "snow.schema.name": "TELEMETRY",
  "snow.session.id": 2533678517649562,
  "snow.session.role.primary.id": 4,
  "snow.session.role.primary.name": "SYSADMIN",
  "snow.user.id": 1,
  "snow.warehouse.id": 40,
  "snow.warehouse.name": "TELEMETRY_WH",
  "telemetry.sdk.language": "python"
}
*/


----- 2. TRACING -----

use role sysadmin;
use database aqi_dev_db;
use schema telemetry;
use warehouse telemetry_wh;

alter warehouse telemetry_wh set warehouse_size='SMALL';

-- Follow : https://opentelemetry.io/
create or replace event table trace_events;

show event tables;

desc event table trace_events; 

-- alter account set event_table = aqi_dev_db.telemetry.trace_events;
-- show parameters like 'event_table' in account;

CREATE OR REPLACE FUNCTION py_sample_function()
RETURNS number
LANGUAGE python
RUNTIME_VERSION= 3.10
packages = ('snowflake-telemetry-python')
handler = 'basic_fn'
AS
$$
from snowflake import telemetry
import logging

logger = logging.getLogger("py Logger")

def basic_fn():
    logger.info("Sample INFO log")
    return 0

$$;


alter function py_sample_function() set log_level= TRACE;
alter function py_sample_function() set trace_level= ALWAYS; -- ALWAYS,ON_EVENT,PROPAGATE,OFF.

select py_sample_function();


select * from trace_events;


create or replace function india_2025_tax_calculator(employee_salary number, tax_rebate_code char, exemption_amount number)
RETURNS number
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
packages = ('snowflake-telemetry-python', 'psutil')
HANDLER = 'calculate_tax'
AS $$
import time
import logging
from snowflake import telemetry
from opentelemetry import trace 
import psutil
import platform
import socket

logger = logging.getLogger("CPU Logger")

def ip_info():
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        logger.info(f"Local IP Address: {ip_address}")
    except socket.error as e:
        logger.info(f"Could not get IP address: {e}")


def get_size(bytes, suffix="B"):
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor

def get_ec2_instance_info(region_name='us-east-2'):
    try:
        ec2_client = boto3.client('ec2', region_name=region_name)
        response = ec2_client.describe_instances()

        logger.info(f"EC2 Instance Information in {region_name}:")
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                instance_type = instance['InstanceType']
                instance_state = instance['State']['Name']
                public_ip = instance.get('PublicIpAddress', 'N/A')
                private_ip = instance.get('PrivateIpAddress', 'N/A')

                instance_name = 'N/A'
                if 'Tags' in instance:
                    for tag in instance['Tags']:
                        if tag['Key'] == 'Name':
                            instance_name = tag['Value']
                            break

                logger.info(f"\n  Instance ID: {instance_id}")
                logger.info(f"  Name: {instance_name}")
                logger.info(f"  Type: {instance_type}")
                logger.info(f"  State: {instance_state}")
                logger.info(f"  Public IP: {public_ip}")
                logger.info(f"  Private IP: {private_ip}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


def cpu_info():
    logger.info(f"======================CPU Information======================")
    logger.info(f"Physical Cores: {psutil.cpu_count(logical=False)}")
    logger.info(f"Logical Cores (vCPUs): {psutil.cpu_count(logical=True)}")

    logger.info(f"======================Memory Information======================")
    svmem = psutil.virtual_memory()
    logger.info(f"Total Memory: {get_size(svmem.total)}")
    logger.info(f"Available Memory: {get_size(svmem.available)}")
    logger.info(f"Used Memory: {get_size(svmem.used)}")
    logger.info(f"Memory Usage: {svmem.percent}%")


def calculate_tax(employee_salary, tax_rebate_code, exemption_amount):
    # get_ec2_instance_info()
    ip_info()
    # cpu_info()
    tracer = trace.get_tracer("TVK-Simplied")
    with tracer.start_as_current_span("vin.span") as span:
        span.add_event("DE-Simplified Learning", {"employee_salary": employee_salary, "tax_rebate_code": tax_rebate_code, "exemption_amount": exemption_amount})

    telemetry.set_span_attribute('vinodh.name', 'vin****')
    telemetry.set_span_attribute('vinodh.phone', '7****2')
    telemetry.set_span_attribute('vinodh.email', 'vin****@gmail.com')
    telemetry.add_event('Empty Event')
    time.sleep(10)
    telemetry.add_event("Event With Attributes", {"employee_salary": employee_salary, "tax_rebate_code": tax_rebate_code, "exemption_amount": exemption_amount})
    time.sleep(10)
    rebate_percentage = {
        'A': 0.10,
        'B': 0.15,
        'C': 0.20
    }

    tax_rabate_code_pct=0.30

    if tax_rebate_code in rebate_percentage:
        tax_rabate_code_pct = rebate_percentage[tax_rebate_code]

    rebate_amount = employee_salary * tax_rabate_code_pct
    exemption_pct = (exemption_amount/employee_salary)
    total_sal_with_std_deductions = employee_salary-30000
    telemetry.add_event("Event With More Attributes", {"rebate_amount": rebate_amount, "exemption_pct": exemption_pct, "total_sal_with_std_deductions": total_sal_with_std_deductions})

    taxable_income=0
    if exemption_pct > 0.3:
        taxable_income = total_sal_with_std_deductions - rebate_amount - exemption_amount
    else:
        taxable_income = total_sal_with_std_deductions - rebate_amount
    return max(taxable_income, 0)

$$;

ALTER function india_2025_tax_calculator(number, char, number) set LOG_LEVEL=TRACE;
ALTER function india_2025_tax_calculator(number, char, number) set TRACE_LEVEL=ALWAYS;

SELECT india_2025_tax_calculator(3000000, 'C', 400000) AS taxable_income;


create or replace transient table employee(
    id int primary key,
    name text,
    salary number,
    tax_code char,
    tax_exemption number
);

insert into employee values 
    (101, 'Alice 1', 20000, 'A', 5000),
    (102, 'Alice 2', 50000, 'B', 7000),
    (103, 'Alice 3', 60000, 'C', 4000),
    (104, 'Alice 4', 30000, 'A', 8000),
    (105, 'Alice 5', 60000, 'B', 5000),
    (106, 'Alice 6', 80000, 'C', 4000),
    (107, 'Alice 7', 10000, 'A', 2000),
    (108, 'Alice 8', 90000, 'B', 8000),
    (109, 'Alice 9', 60000, null, 9000),
    (110, 'Alice 10', 0, 'X', 5500)
;

SELECT id, name, salary, tax_code, tax_exemption,
    india_2025_tax_calculator(salary, tax_code, tax_exemption) AS taxable_income1,
    india_2025_tax_calculator(salary+(0.1*salary), tax_code, tax_exemption) AS taxable_income2
    from employee where id between 101 and 109;


select 
    round((sum(bytes_ingested)/1024)/1024, 2) as total_MB_storage,
    round(sum(credits_used), 5) as total_credits,
    round(total_credits*3,2) as total_enterprise_dollars
from snowflake.account_usage.event_usage_history;

----- 3. METRICS -----

create or replace event table metric_events;

-- alter account set event_table= aqi_dev_db.telemetry.metric_events;

select * from metric_events;

create or replace procedure heavy_transformation_sp()
RETURNS text
LANGUAGE SQL
AS
BEGIN

    SYSTEM$ADD_EVENT('Starting Event');

    LET distinct_query := 'select distinct inv_warehouse_sk from snowflake_sample_data.tpcds_sf100tcl.inventory;';
    LET agg_query      := 'select sum(inv_quantity_on_hand) from snowflake_sample_data.tpcds_sf100tcl.inventory where inv_warehouse_sk IN (11, 2, 14);';
    LET filter_query   := 'select ca_county, ca_city, count(1) from snowflake_sample_data.tpcds_sf100tcl.customer_address where ca_state in (''NE'', ''IA'', ''TX'') group by all;';
    LET load_query     := 'select * from snowflake_sample_data.tpcds_sf100tcl.customer_address order by ca_county, ca_city desc;';

    SYSTEM$ADD_EVENT('Queries Defined');

    EXECUTE IMMEDIATE distinct_query;
    SYSTEM$ADD_EVENT('distinct_query Done');

    EXECUTE IMMEDIATE agg_query;
    SYSTEM$ADD_EVENT('agg_query Done');

    EXECUTE IMMEDIATE filter_query;
    SYSTEM$ADD_EVENT('filter_query Done');

    EXECUTE IMMEDIATE load_query;
    SYSTEM$ADD_EVENT('load_query Done');

    SYSTEM$ADD_EVENT('End Event');

    return 'SUCCESS';

EXCEPTION
    WHEN EXPRESSION_ERROR THEN SYSTEM$LOG_ERROR('SP with EXPRESSION_ERROR'); RETURN 'EXPRESSION_ERROR';
    WHEN STATEMENT_ERROR THEN SYSTEM$LOG_ERROR('SP with STATEMENT_ERROR'); RETURN 'STATEMENT_ERROR';
    WHEN OTHER THEN SYSTEM$LOG_ERROR('SP with OTHER'); RETURN 'OTHER';
END;

ALTER SESSION SET LOG_LEVEL=TRACE; -- TRACE,DEBUG,INFO,WARN,ERROR,FATAL,OFF.
ALTER SESSION SET TRACE_LEVEL=ALWAYS; -- ALWAYS,ON_EVENT,PROPAGATE,OFF.
ALTER SESSION SET METRIC_LEVEL=ALL; -- ALL, NONE

call heavy_transformation_sp();

show warehouses;

CREATE OR REPLACE WAREHOUSE TELEMETRY_MEDIUM_SNOWPARK_WH 
  WITH 
  WAREHOUSE_SIZE = 'MEDIUM'
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  AUTO_SUSPEND = 100
  AUTO_RESUME = FALSE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 5
  SCALING_POLICY = 'STANDARD'
  ENABLE_QUERY_ACCELERATION = TRUE
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 0;

CREATE OR REPLACE WAREHOUSE TELEMETRY_LARGE_SNOWPARK_WH 
  WITH 
  WAREHOUSE_SIZE = 'LARGE'
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  AUTO_SUSPEND = 100
  AUTO_RESUME = FALSE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 5
  SCALING_POLICY = 'STANDARD'
  ENABLE_QUERY_ACCELERATION = TRUE
  QUERY_ACCELERATION_MAX_SCALE_FACTOR = 0;


CREATE OR REPLACE PROCEDURE PY_HEAVY_TRANSFORMATIONS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
HANDLER = 'etl'
AS $$
import logging
from snowflake import telemetry
from opentelemetry import trace

logger = logging.getLogger('Py Logger')

def etl(session):
    telemetry.add_event('Handler Started.');
    try:
        distinct_query = "select distinct inv_warehouse_sk from snowflake_sample_data.tpcds_sf100tcl.inventory;";
        telemetry.add_event("FirstQuery", {"distinct_query": distinct_query});
        agg_query      = "select sum(inv_quantity_on_hand) from snowflake_sample_data.tpcds_sf100tcl.inventory where inv_warehouse_sk IN (11, 2, 14);";
        telemetry.add_event("SecondQuery", {"agg_query": agg_query});
        filter_query   = "select ca_county, ca_city, count(1) from snowflake_sample_data.tpcds_sf100tcl.customer_address where ca_state in ('NE', 'IA', 'TX') group by all;";
        telemetry.add_event("ThirdQuery", {"filter_query": filter_query});
        load_query     = "select * from snowflake_sample_data.tpcds_sf100tcl.customer_address order by ca_county, ca_city desc;";
        telemetry.add_event("FourthQuery", {"load_query": load_query});

        session.sql(distinct_query).collect();
        telemetry.add_event('distinct_query Done.')
        session.sql(agg_query).collect();
        telemetry.add_event('agg_query Done.')
        session.sql(filter_query).collect();
        telemetry.add_event('filter_query Done.')
        session.sql(load_query).collect();
        telemetry.add_event('load_query Done.')
        
        telemetry.add_event('Handler Done.')

        return "SUCCESS"
    
    except Exception as e:
        if "EXPRESSION_ERROR" in str(e):
            logger.error(f"Failed with EXPRESSION_ERROR \s  {str(e)}")
            return f"EXPRESSION_ERROR - {str(e)}"
        elif "STATEMENT_ERROR" in str(e):
            logger.error(f"Failed with STATEMENT_ERROR \s  {str(e)}")
            return f"STATEMENT_ERROR - {str(e)}"
        else:
            logger.error(f"Failed with OTHER \s  {str(e)}")
            return f"OTHER - {str(e)}"       
$$;

ALTER WAREHOUSE TELEMETRY_WH RESUME;

ALTER PROCEDURE PY_HEAVY_TRANSFORMATIONS() SET METRIC_LEVEL=ALL;
ALTER PROCEDURE PY_HEAVY_TRANSFORMATIONS() SET TRACE_LEVEL=ALWAYS;
ALTER PROCEDURE PY_HEAVY_TRANSFORMATIONS() SET LOG_LEVEL=TRACE;

call PY_HEAVY_TRANSFORMATIONS();


SELECT
  DISTINCT
  RECORD_TYPE, 
  TIMESTAMP AS time,
  RESOURCE_ATTRIBUTES['snow.executable.name']::TEXT as executable,
  RECORD['severity_text']::TEXT AS severity,
  VALUE AS message
FROM
  metric_events;



CREATE OR REPLACE PROCEDURE DEMO_SP(n_queries number)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python==0.2.0')
HANDLER = 'my_handler'
AS $$
import time
def my_handler(session, n_queries):
  import snowflake.snowpark
  from snowflake.snowpark.functions import col, udf
  from snowflake import telemetry

  session.sql('create or replace stage udf_stage;').collect()

  # Temp UDF's are NOT supported inside Procedure ? Need to confirm !!  
  @udf(name='example_udf', is_permanent=True, stage_location='@udf_stage', replace=True)
  def example_udf(x: int) -> int:
    # This UDF will consume 1GB of memory to illustrate the memory consumption metric
    one_gb_list = [0] * (1024**3 // 8)
    return x

  pandas_grouped_df = session.table('snowflake.account_usage.query_history').select(
    col('total_elapsed_time'),
    col('rows_written_to_result'),
    col('database_name'),
    example_udf(col('bytes_scanned'))
  ).limit(n_queries)\
  .to_pandas()\
  .groupby('DATABASE_NAME')

  mean_time = pandas_grouped_df['TOTAL_ELAPSED_TIME'].mean()
  mean_rows_written = pandas_grouped_df['ROWS_WRITTEN_TO_RESULT'].mean()

  return f"""
  {mean_time}
  {mean_rows_written}
  """
$$;

ALTER SESSION SET METRIC_LEVEL = ALL;

-- use role accountadmin;
call DEMO_SP(1000);

 