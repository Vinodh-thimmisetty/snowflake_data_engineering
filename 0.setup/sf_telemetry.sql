-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;


show parameters like 'event_table' in account;

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