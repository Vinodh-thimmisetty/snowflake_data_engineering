SELECT SYSTEM$ENABLE_BEHAVIOR_CHANGE_BUNDLE('2025_05');

SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2025_05');

SELECT SYSTEM$SHOW_ACTIVE_BEHAVIOR_CHANGE_BUNDLES();


use role accountadmin;


CREATE OR REPLACE TRANSIENT TABLE car_repairs AS
WITH RECURSIVE customers AS (
    SELECT 'A' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 3, '0') AS customer_code
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
),
cars AS (
    SELECT customer_code,
           '2025-08-01'::DATE AS run_month,
           'ABC' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4()), 5, '0') AS car
    FROM customers,
         TABLE(GENERATOR(ROWCOUNT => 100000))
)
SELECT customer_code, run_month, car
FROM cars;
 
CREATE OR REPLACE PROCEDURE repair_by_CUSTOMER(
    customer_code VARCHAR(4),
    run_month DATE,
    total_records OUT INT,
    current_query_id OUT VARCHAR)
  RETURNS STRING
  LANGUAGE SQL
AS
$$ 
BEGIN
  (SELECT COUNT(1) INTO total_records FROM IDENTIFIER('car_repairs')
    WHERE customer_code =:customer_code AND
          run_month = :run_month);
  current_query_id := LAST_QUERY_ID();
  RETURN 'Done';
END;
$$
;

-- SEQUENTIAL
CREATE OR REPLACE PROCEDURE find_car_repairs(customers ARRAY, run_month VARCHAR )
RETURNS VARIANT
LANGUAGE SQL
AS
DECLARE
    start_val int default 0; 
    end_val int default ARRAY_SIZE(customers) - 1;
    result_val VARIANT DEFAULT ARRAY_CONSTRUCT();
    parent_session_id VARCHAR DEFAULT CURRENT_SESSION();
BEGIN
  FOR idx IN start_val TO end_val DO
    LET child_session_id VARCHAR DEFAULT CURRENT_SESSION();
    LET current_CUSTOMER_total_assets := 0; 
    LET current_CUSTOMER_query_id VARCHAR DEFAULT NULL; 
    LET metadata_info OBJECT := NULL;
    LET p_query_id VARCHAR;
    LET query_time_seconds FLOAT;
    LET query_warehouse VARCHAR;
    LET query_cluster_number NUMBER;
    LET current_CUSTOMER VARCHAR := GET(customers, idx);
    CALL repair_by_CUSTOMER(:current_CUSTOMER, :run_month, :current_CUSTOMER_total_assets, :current_CUSTOMER_query_id);
    let procedure_query_id VARCHAR := LAST_QUERY_ID();
    -- select (TOTAL_ELAPSED_TIME / 1000), WAREHOUSE_NAME, CLUSTER_NUMBER INTO :query_time_seconds, :query_warehouse, :query_cluster_number from TABLE(DUMMY.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(:child_session_id)) WHERE QUERY_ID = :ttt;
    metadata_info := OBJECT_CONSTRUCT(
        '0.is_same_session', (:parent_session_id = :child_session_id),
        '1.parent_session_id', :parent_session_id,
        '2.child_session_id', :child_session_id,
        '3.customer_code', :current_CUSTOMER,
        '4.run_month', :run_month,
        '5.total_assets', :current_CUSTOMER_total_assets, 
        '6.CUSTOMER_query_id', :current_CUSTOMER_query_id,
        '7.proc_query_id', :procedure_query_id
        -- '7.time_taken(seconds)', :query_time_seconds,
        -- '8.warehouse', :query_warehouse,
        -- '9.wh cluster', :query_cluster_number
      ); 
    result_val := ARRAY_APPEND(result_val, metadata_info);
  END FOR;
  RETURN result_val;
END;

CALL find_car_repairs(ARRAY_CONSTRUCT('CUST123','CUST456','CUST789'),  '2025-08-01');

-- ASYNC
CREATE OR REPLACE PROCEDURE find_car_repairs_async(customers ARRAY, run_month VARCHAR )
RETURNS NUMBER
LANGUAGE SQL
AS
DECLARE
    start_val int default 0; 
    end_val int default ARRAY_SIZE(customers) - 1;
    result_val INT DEFAULT 0;
    parent_session_id VARCHAR DEFAULT CURRENT_SESSION();
BEGIN
  FOR idx IN start_val TO end_val DO
    LET child_session_id VARCHAR DEFAULT CURRENT_SESSION();
    LET total_assets := 0; 
    LET current_CUSTOMER VARCHAR := GET(customers, idx);
    ASYNC (CALL repair_by_CUSTOMER(:current_CUSTOMER, :run_month, :total_assets));
    SYSTEM$LOG_INFO( 
      OBJECT_CONSTRUCT(
        'parent_session_id', :parent_session_id,
        'child_session_id', :child_session_id,
        'customer_code', :current_CUSTOMER,
        'run_month', :run_month,
        'total_assets', :total_assets
      )
    );

    result_val := result_val + total_assets;
  END FOR;
  AWAIT ALL;
  RETURN result_val;
END;

CALL find_car_repairs_async(ARRAY_CONSTRUCT('CUST123','CUST456','CUST789'),  '2025-08-01');


CREATE OR REPLACE PROCEDURE test_sp_async_child_jobs_insert()
RETURNS VARCHAR
LANGUAGE SQL
AS
  BEGIN
	ASYNC (INSERT INTO my_table VALUES ('Parallel 1'));
	ASYNC (INSERT INTO my_table VALUES ('Parallel 2'));
	
-- Ensures all insertions finish before returning ---- 
	AWAIT ALL

	RETURN 'Done (Async)';
  END;

  
CREATE OR REPLACE TRANSIENT TABLE REPAIR_EVENT_LOG (
    event_log_id VARCHAR,
    category VARCHAR,
    customer_code VARCHAR(4),
    run_month DATE,
    total_records INT,
    parent_query_id VARCHAR,
    current_query_id VARCHAR,
    parent_session_id VARCHAR,
    current_session_id VARCHAR,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE customer_repair_with_wait(
    event_log_tablename VARCHAR,
    wait_time_in_seconds INT,
    event_log_id VARCHAR,
    customer_code VARCHAR(4),
    run_month DATE)
  RETURNS STRING
  LANGUAGE SQL
AS
BEGIN
  -- IF (:customer_code ='A123') THEN  
  --   CALL SYSTEM$WAIT(100);
  -- ELSEIF (:customer_code ='B123') THEN  
  --   CALL SYSTEM$WAIT(200);
  -- ELSEIF (:customer_code ='B123') THEN  
  --   CALL SYSTEM$WAIT(0);
  -- ELSE 
    CALL SYSTEM$WAIT(:wait_time_in_seconds);
  -- END IF;
  LET total_records INT := (SELECT COUNT(1) FROM IDENTIFIER('vinodh_repair_events')
    WHERE customer_code =:customer_code AND
          run_month = :run_month AND 
          LENGTH(car_number)=8 AND LEFT(car_number, 3) RLIKE '^[A-Za-z]{3}$' AND RIGHT(car_number, 5) RLIKE '^[0-9]{5}$'
          );
  UPDATE IDENTIFIER(:event_log_tablename) SET total_records=:total_records, current_query_id=LAST_QUERY_ID(), current_session_id=CURRENT_SESSION() WHERE event_log_id = :event_log_id;
  -- CALL SYSTEM$WAIT(:wait_time_in_seconds);
  RETURN 'Done';
END;


-- SEQUENTIAL
CREATE OR REPLACE PROCEDURE generate_repairs_sequential(event_log_tablename VARCHAR, wait_time_in_seconds INT, customers ARRAY, run_month VARCHAR )
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    start_val int default 0; 
    end_val int default ARRAY_SIZE(customers) - 1;
BEGIN
  FOR idx IN start_val TO end_val DO
    LET current_customer VARCHAR := GET(customers, idx);
    LET event_log_id := UUID_STRING();  
    INSERT INTO IDENTIFIER(:event_log_tablename)(event_log_id, category, customer_code, run_month) VALUES(:event_log_id, 'SEQUENTIAL', :current_customer, :run_month);
    CALL customer_repair_with_wait(:event_log_tablename, :wait_time_in_seconds, :event_log_id, :current_customer, :run_month);
    UPDATE IDENTIFIER(:event_log_tablename) SET parent_query_id=LAST_QUERY_ID(), parent_session_id=CURRENT_SESSION() WHERE event_log_id= :event_log_id;
  END FOR;
  RETURN 'OKAY';
END;

-- ASYNC
CREATE OR REPLACE PROCEDURE generate_repairs_async(event_log_tablename VARCHAR, wait_time_in_seconds INT, customers ARRAY, run_month VARCHAR )
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    start_val int default 0; 
    end_val int default ARRAY_SIZE(customers) - 1;
BEGIN
  FOR idx IN start_val TO end_val DO
    LET current_customer VARCHAR := GET(customers, idx);
    LET event_log_id := UUID_STRING();
    -- IF (:current_customer ='C123') THEN  
      -- CALL SYSTEM$WAIT(20);
    -- END IF;

    INSERT INTO IDENTIFIER(:event_log_tablename)(event_log_id, category, customer_code, run_month) VALUES(:event_log_id, 'ASYNC', :current_customer, :run_month);
    ASYNC(
    CALL customer_repair_with_wait(:event_log_tablename, :wait_time_in_seconds, :event_log_id, :current_customer, :run_month)
    );
    UPDATE IDENTIFIER(:event_log_tablename) SET parent_query_id=LAST_QUERY_ID(), parent_session_id=CURRENT_SESSION() WHERE event_log_id= :event_log_id;
  END FOR;
  AWAIT ALL;
  RETURN 'OKAY';
END;

WITH last_query_ids as (
    SELECT 
    r.value:"1.customer_code"::TEXT as customer,
    r.value:"7.parent_query_id"::TEXT AS parent_query_id,
    r.value:"8.child_query_id"::TEXT AS child_query_id
FROM
    TABLE(RESULT_SCAN(LAST_QUERY_ID())) as t,
    LATERAL FLATTEN(INPUT => t.GENERATE_REPAIRS) AS r
) 
SELECT
    customer,
    QUERY_ID,
    QUERY_TEXT,
    TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    WAREHOUSE_TYPE,
    CLUSTER_NUMBER,
    EXECUTION_STATUS
FROM
   TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
INNER JOIN last_query_ids ON QUERY_ID=parent_query_id OR QUERY_ID=child_query_id;


-- TESTING
-- call customer_repair_with_wait('REPAIR_EVENT_LOG_MULTI_WH', 'b05ed7ba-39fd-448d-b2fa-da8f9026541c', 'A123', '2025-08-01');

USE WAREHOUSE COMPUTE_WH;
ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE COMPUTE_WH RESUME;
DELETE FROM REPAIR_EVENT_LOG;
SET REPAIR_LOG_TABLE_NAME='REPAIR_EVENT_LOG_MULTI_WH';
SET SQL_WAIT_TIME_IN_SECONDS=15;
CALL generate_repairs_sequential($REPAIR_LOG_TABLE_NAME, $SQL_WAIT_TIME_IN_SECONDS, ARRAY_CONSTRUCT('A123','B123','C123'),  '2025-08-01'); 
CALL generate_repairs_async($REPAIR_LOG_TABLE_NAME, $SQL_WAIT_TIME_IN_SECONDS, ARRAY_CONSTRUCT('A123','B123','C123'),  '2025-08-01'); 
SELECT * FROM REPAIR_EVENT_LOG; 




USE WAREHOUSE COMPUTE_MULTI_WH; -- Multi Cluster Enabled with MIN 1 and MAX 5
ALTER WAREHOUSE COMPUTE_MULTI_WH SUSPEND; -- To Skip Warehouse level CACHE in COMPUTE LAYER
ALTER SESSION SET USE_CACHED_RESULT = FALSE;-- To Skip CACHE in Cloud Service Layer 
ALTER WAREHOUSE COMPUTE_MULTI_WH RESUME;
DELETE FROM REPAIR_EVENT_LOG; 
SET REPAIR_LOG_TABLE_NAME ='REPAIR_EVENT_LOG';
SET SQL_WAIT_TIME_IN_SECONDS=15; -- MIMIC LOGIC EXECUTION TIME
CALL generate_repairs_sequential($REPAIR_LOG_TABLE_NAME, $SQL_WAIT_TIME_IN_SECONDS, ARRAY_CONSTRUCT('A123','B123','C123'),  '2025-08-01'); 
CALL generate_repairs_async($REPAIR_LOG_TABLE_NAME, $SQL_WAIT_TIME_IN_SECONDS,ARRAY_CONSTRUCT('A123','B123','C123'),  '2025-08-01'); 
SELECT * FROM REPAIR_EVENT_LOG; 

 
 
SELECT
    customer_CODE,
    CATEGORY,
    IFF(QUERY_ID=PARENT_QUERY_ID,PARENT_QUERY_ID, NULL) AS PARENT_QUERY_ID,
    IFF(QUERY_ID=CURRENT_QUERY_ID,CURRENT_QUERY_ID, NULL) AS CURRENT_QUERY_ID,
    TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    WAREHOUSE_TYPE,
    CLUSTER_NUMBER
FROM
   TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
INNER JOIN REPAIR_EVENT_LOG_MULTI_WH  ON QUERY_ID=CURRENT_QUERY_ID OR QUERY_ID=PARENT_QUERY_ID
ORDER BY 1, 2 DESC, 3, 4 NULLS LAST;