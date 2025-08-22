-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;


WITH emp_bonus AS PROCEDURE (emp_id IN number, table_name IN varchar)
  -- RETURNS NUMBER
  RETURNS TABLE()
  LANGUAGE SQL
  AS
  $$
  DECLARE
    row_count NUMBER DEFAULT (SELECT COUNT(*) FROM IDENTIFIER(:table_name) WHERE emp_id = :emp_id);
    res RESULTSET;
  BEGIN
    IF (emp_id IS NOT NULL) THEN
        -- res := (SELECT * FROM IDENTIFIER(:table_name) WHERE emp_id = :emp_id);
        let select_statement VARCHAR := 'SELECT * FROM IDENTIFIER(\'' || table_name ||'\') WHERE emp_id = ' || emp_id;
        res := (EXECUTE IMMEDIATE :select_statement);
        IF (row_count = 0) THEN
        -- IF (NOT EXISTS (SELECT 1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1)) THEN
            res := (SELECT * FROM IDENTIFIER(:table_name));
            row_count :=  (SELECT COUNT(1) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        END IF;
    END IF;
    
    RETURN TABLE(res);
  END;
  $$
CALL emp_bonus(10011, 'DUMMY.TMP.BONUSES');



CREATE OR REPLACE PROCEDURE multiple_out_sp_demo(
    p1_in NUMBER,
    p1_out OUT NUMBER,
    p2_in VARCHAR(100),
    p2_out OUT VARCHAR(100),
    p3_in BOOLEAN,
    p3_out OUT BOOLEAN)
  RETURNS NUMBER
  LANGUAGE SQL
AS
BEGIN
  p1_in := p1_in + 1;
  p1_out := p1_out + 1;
  p2_in := p2_in || ' hi ';
  p2_out := p2_out || ' hi ';
  p3_in := (NOT p3_in);
  p3_out := (NOT p3_out);
  RETURN 1;
END;

BEGIN
  LET x_in INT := 1;
  LET x_out INT := 1;
  LET y_in VARCHAR(100) := 'hello';
  LET y_out VARCHAR(100) := 'hello';
  LET z_in BOOLEAN := true;
  LET z_out BOOLEAN := true;

  CALL multiple_out_sp_demo(:x_in, :x_out, :y_in, :y_out, :z_in, :z_out);
  CALL multiple_out_sp_demo(:x_in, :x_out, :y_in, :y_out, :z_in, :z_out);
  CALL multiple_out_sp_demo(:x_in, :x_out, :y_in, :y_out, :z_in, :z_out);
  RETURN [x_in, x_out, y_in, y_out, z_in, z_out];
END;


CREATE OR REPLACE TABLE quarterly_sales(
  empid INT,
  amount INT,
  quarter TEXT)
  AS SELECT * FROM VALUES
    (1, 10000, '2023_Q1'),
    (1, 400, '2023_Q1'),
    (2, 4500, '2023_Q1'),
    (2, 35000, '2023_Q1'),
    (1, 5000, '2023_Q2'),
    (1, 3000, '2023_Q2'),
    (2, 200, '2023_Q2'),
    (2, 90500, '2023_Q2'),
    (1, 6000, '2023_Q3'),
    (1, 5000, '2023_Q3'),
    (2, 2500, '2023_Q3'),
    (2, 9500, '2023_Q3'),
    (3, 2700, '2023_Q3'),
    (1, 8000, '2023_Q4'),
    (1, 10000, '2023_Q4'),
    (2, 800, '2023_Q4'),
    (2, 4500, '2023_Q4'),
    (3, 2700, '2023_Q4'),
    (3, 16000, '2023_Q4'),
    (3, 10200, '2023_Q4');

CREATE OR REPLACE PROCEDURE sales_total_out_sp_demo(
    ids ARRAY,
    quarter VARCHAR(20),
    total_sales OUT NUMBER(38,0))
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
  SELECT SUM(amount) INTO total_sales FROM quarterly_sales
    WHERE empid IN (** :ids) AND
          quarter = :quarter;
  RETURN 'Done';
END;
$$
;

CREATE OR REPLACE PROCEDURE sales_total_out_sp_demo2(
    id NUMBER,
    quarter VARCHAR(20),
    total_sales OUT NUMBER(38,0))
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
  SELECT SUM(amount) INTO total_sales FROM quarterly_sales
    WHERE empid =:id AND
          quarter = :quarter;
  RETURN 'Done';
END;
$$
;

show parameters like 'event_table' in account;
create or replace event table sp_events;
ALTER ACCOUNT SET ENABLE_ACCOUNT_USAGE_DATA_SHARING = TRUE;
ALTER ACCOUNT SET EVENT_TABLE = dummy.tmp.sp_events;
-- Set the trace level for the entire account
ALTER ACCOUNT SET TRACE_LEVEL = ALWAYS;
ALTER ACCOUNT SET LOG_LEVEL = INFO;
ALTER ACCOUNT SET METRIC_LEVEL = ALL;


CREATE OR REPLACE PROCEDURE emp_quarter_calling_sp_demo(array_ids ARRAY, quarter VARCHAR )
RETURNS NUMBER
LANGUAGE SQL
AS
DECLARE
    start_val int default 0; 
    end_val int default ARRAY_SIZE(array_ids) - 1;
    result_val INT DEFAULT 0;
    session_id VARCHAR DEFAULT CURRENT_SESSION();
BEGIN
  FOR i IN start_val TO end_val DO
    LET sales := 0; 
    LET current_value NUMBER := GET(array_ids, i);
    CALL sales_total_out_sp_demo2(:current_value, :quarter, :sales);
    
    SYSTEM$LOG_INFO( 
      OBJECT_CONSTRUCT(
        'session_id', :session_id,
        'employee_id', :current_value,
        'quarter', :quarter,
        'sales_result', :sales
      )
    );

    result_val := result_val + sales;
  END FOR;
  RETURN result_val;
END;

select * from quarterly_sales;

CALL emp_quarter_calling_sp_demo(ARRAY_CONSTRUCT(1,2, 3), '2023_Q4');

SHOW PARAMETERS LIKE 'trace_level' IN ACCOUNT;
SHOW PARAMETERS LIKE 'log_level' IN ACCOUNT;
SHOW PARAMETERS LIKE 'metric_level' IN ACCOUNT;




DECLARE
  query_id_1 VARCHAR;
  query_id_2 VARCHAR;
BEGIN
  SELECT 1;
  query_id_1 := SQLID;
  SELECT 1;
  query_id_2 := SQLID;
  RETURN [query_id_1, query_id_2];
END;

CREATE OR REPLACE DATABASE DUMMY;

CREATE OR REPLACE SCHEMA TMP WITH MANAGED ACCESS;

CREATE OR REPLACE TRANSIENT TABLE my_values (value NUMBER);

/*
 
SQLROWCOUNT : Number of rows affected by the last DML statement.
SQLFOUND : true if the last DML statement affected one or more rows.
SQLNOTFOUND : true if the last DML statement affected zero rows

*/
EXECUTE IMMEDIATE $$
BEGIN
  LET sql_row_count_var INT := 0;
  INSERT INTO my_values VALUES (1), (2), (3);
  sql_row_count_var := SQLROWCOUNT;
  RETURN sql_row_count_var;
END;
$$;

EXECUTE IMMEDIATE $$
BEGIN
  LET sql_row_count_var INT := 0;
  LET sql_found_var BOOLEAN := NULL;
  LET sql_notfound_var BOOLEAN := NULL;
  IF ((SELECT MAX(value) FROM my_values) > 2) THEN
    UPDATE my_values SET value = 4 WHERE value < 3;
    sql_row_count_var := SQLROWCOUNT;
    sql_found_var := SQLFOUND;
    sql_notfound_var := SQLNOTFOUND;
  END IF;
  SELECT * from my_values;
  IF (sql_found_var = true) THEN
    RETURN 'Updated ' || sql_row_count_var || ' rows.';
  ELSEIF (sql_notfound_var = true) THEN
    RETURN 'No rows updated.';
  ELSE
    RETURN 'No DML statements executed.';
  END IF;
END;
$$;


CREATE OR REPLACE TABLE bonuses (
  emp_id INT,
  performance_rating INT,
  salary NUMBER(12, 2),
  bonus NUMBER(12, 2)
);

INSERT INTO bonuses (emp_id, performance_rating, salary, bonus) VALUES
  (1001, 3, 100000, NULL),
  (1002, 1, 50000, NULL),
  (1003, 4, 75000, NULL),
  (1004, 4, 80000, NULL),
  (1005, 5, 120000, NULL),
  (1006, 2, 60000, NULL),
  (1007, 5, 40000, NULL),
  (1008, 3, 140000, NULL),
  (1009, 1, 95000, NULL);

SELECT * FROM bonuses;

CREATE OR REPLACE PROCEDURE apply_bonus(bonus_percentage INT, performance_value INT)
  RETURNS TEXT
  LANGUAGE SQL
AS
DECLARE
  -- Use input to calculate the bonus percentage
  updated_bonus_percentage NUMBER(2,2) DEFAULT (:bonus_percentage/100);
  --  Declare a result set
  rs RESULTSET;
BEGIN
  -- Assign a query to the result set and execute the query
  rs := (SELECT * FROM bonuses);
  -- Use a FOR loop to iterate over the records in the result set
  FOR record IN rs DO
    -- Assign variable values using values in the current record
    LET emp_id_value INT := record.emp_id;
    LET performance_rating_value INT := record.performance_rating;
    LET salary_value NUMBER(12, 2) := record.salary;
    -- Determine whether the performance rating in the record matches the user input
    IF (performance_rating_value = :performance_value) THEN
      -- If the condition is met, update the bonuses table using the calculated bonus percentage
      UPDATE bonuses SET bonus = ( :salary_value * :updated_bonus_percentage )
        WHERE emp_id = :emp_id_value;
    END IF;
  END FOR;
  -- Return text when the stored procedure completes
  RETURN 'Update applied';
END;

CALL apply_bonus(3, 5);


SELECT * FROM bonuses;


CREATE OR REPLACE TABLE vm_ownership (
  emp_id INT,
  vm_id VARCHAR
);

INSERT INTO vm_ownership (emp_id, vm_id) VALUES
  (1001, 1),
  (1001, 5),
  (1002, 3),
  (1003, 4),
  (1003, 6),
  (1003, 2);

CREATE OR REPLACE TABLE vm_settings (
  vm_id INT,
  vm_setting VARCHAR,
  value NUMBER
);

INSERT INTO vm_settings (vm_id, vm_setting, value) VALUES
  (1, 's1', 5),
  (1, 's2', 500),
  (2, 's1', 10),
  (2, 's2', 600),
  (3, 's1', 3),
  (3, 's2', 400),
  (4, 's1', 8),
  (4, 's2', 700),
  (5, 's1', 1),
  (5, 's2', 300),
  (6, 's1', 7),
  (6, 's2', 800);

CREATE OR REPLACE TABLE vm_settings_history (
  vm_id INT,
  vm_setting VARCHAR,
  value NUMBER,
  owner INT,
  date DATE
);



CREATE OR REPLACE PROCEDURE vm_user_settings()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
DECLARE
  -- Declare a cursor and a variable
  c1 CURSOR FOR SELECT * FROM vm_settings;
  current_owner NUMBER;
BEGIN
  -- Open the cursor to execute the query and retrieve the rows into the cursor
  OPEN c1;
  -- Use a FOR loop to iterate over the records in the result set
  FOR record IN c1 DO
    -- Assign variable values using values in the current record
    LET current_vm_id NUMBER := record.vm_id;
    LET current_vm_setting VARCHAR := record.vm_setting;
    LET current_value NUMBER := record.value;
    -- Assign a value to the current_owner variable by querying the vm_ownership table
    SELECT emp_id INTO :current_owner
      FROM vm_ownership
      WHERE vm_id = :current_vm_id;
    -- If the record has a vm_setting equal to 's1', determine whether its value is less than 5
    IF (current_vm_setting = 's1' AND current_value < 5) THEN
      -- If the condition is met, insert a row into the vm_settings_history table
      INSERT INTO vm_settings_history VALUES (
        :current_vm_id,
        :current_vm_setting,
        :current_value,
        :current_owner,
        SYSDATE());
    -- If the record has a vm_setting equal to 's2', determine whether its value is greater than 500
    ELSEIF (current_vm_setting = 's2' AND current_value > 500) THEN
      -- If the condition is met, insert a row into the vm_settings_history table
      INSERT INTO vm_settings_history VALUES (
        :current_vm_id,
        :current_vm_setting,
        :current_value,
        :current_owner,
        SYSDATE());
    END IF;
  END FOR;
  -- Close the cursor
  CLOSE c1;
  -- Return text when the stored procedure completes
  RETURN 'Success';
END;

CALL vm_user_settings();

SELECT * FROM vm_settings_history ORDER BY vm_id;


DROP TABLE my_values ;
DROP TABLE bonuses ;
DROP TABLE vm_ownership ;
DROP TABLE bonuses ;
DROP TABLE vm_settings_history ;