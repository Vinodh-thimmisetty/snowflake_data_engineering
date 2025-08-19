-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;


-- (variable declarations, cursor declarations, resultsets declaration, exception declaration) ...
DECLARE
          -- my_variable NUMBER DEFAULT 0 ;
          -- my_cursor CURSOR FOR SELECT column FROM <table>;
          -- my_resultset RESULTSET;;
          -- my_exception EXCEPTION (<error_code>, <error description>);
-- (Snowflake Scripting and SQL statements) ...
BEGIN
        -- LET local_variable TEXT  := <local_variable_value>;
        
        --   IF (<condition1>) THEN
        --     RETURN <value-1>;
        --   ELSEIF (<condition1>) THEN
        --     RETURN <value-2>;
        --   ELSE
        --     RETURN <default-value>;
        --   END IF;

        -- IF (should_raise_exception) THEN
        --   RAISE my_exception;
        -- END IF;

        
        --   CASE (expression_to_evaluate)
        --     WHEN <value-1> THEN
        --       RETURN '<result1>';
        --     WHEN <value-2> THEN
        --       RETURN '<result1>';
        --     ELSE
        --       RETURN '<default>';
        --   END;

        --   CASE 
        --     WHEN <condition1> THEN
        --       RETURN '<result1>';
        --     WHEN <condition2> THEN
        --       RETURN '<result1>';
        --     ELSE
        --       RETURN '<default>';
        --   END;
        
        --   FOR i IN <start> TO <end> DO
        --     <execute>;
        --   END FOR;
        
        -- Inner BLOCK  
        -- BEGIN 
            -- supports everything like outer begin/end block
        -- END;
-- (statements for handling exceptions and can read variables in the script) ...
EXCEPTION
  -- WHEN STATEMENT_ERROR THEN
        -- <do something>
        
  -- RETURN <something>;
  
  -- WHEN my_exception THEN
        -- <do something>
  -- RETURN <something>;
  
  -- WHEN OTHER THEN
        -- <do something>
  -- RETURN <something>; (OR) -- RAISE; -- Raise the same exception that you are handling.
  
END;

-- Anonymous BLOCK of CODE
DECLARE
  radius_of_circle FLOAT DEFAULT 3;
  area_of_circle FLOAT;
BEGIN
  area_of_circle := pi() * radius_of_circle * radius_of_circle;
  RETURN area_of_circle;
END; 
EXECUTE IMMEDIATE $$
DECLARE
  radius_of_circle FLOAT;
  area_of_circle FLOAT;
BEGIN
  radius_of_circle := 3;
  area_of_circle := pi() * radius_of_circle * radius_of_circle;
  RETURN area_of_circle;
END; 
$$
;

-- Without Declaration
BEGIN
    LET radius_of_circle := 3;
    RETURN pi() * radius_of_circle * radius_of_circle;
END;

EXECUTE IMMEDIATE $$
BEGIN
    LET radius_of_circle := 3;
    RETURN pi() * radius_of_circle * radius_of_circle;
END;
$$
;


DECLARE
  profit number(38, 2) DEFAULT 0.0;
BEGIN
  LET cost number(38, 2) := 100.0;
  LET revenue number(38, 2) DEFAULT 110.0;

  profit := revenue - cost;
  RETURN profit;
END;


DECLARE
  w INTEGER;
  x INTEGER DEFAULT 0;
  dt DATE;
  result_string VARCHAR;
BEGIN
  w := 1;                     -- Assign a value.
  w := 24 * 7;                -- Assign the result of an expression.
  dt := '2020-09-30'::DATE;   -- Explicit cast.
  dt := '2020-09-30';         -- Implicit cast.
  result_string := w::VARCHAR || ', ' || dt::VARCHAR;
  RETURN result_string;
END;


DECLARE
  counter INTEGER DEFAULT 0;
  maximum_count INTEGER default 5;
BEGIN
  FOR i IN 1 TO maximum_count DO
    counter := counter + 1;
  END FOR;
  RETURN counter;
END;

DECLARE
  counter INTEGER DEFAULT 0;
  maximum_count INTEGER default 5;
BEGIN
  CREATE OR REPLACE TABLE test_for_loop_insert(i INTEGER);
  FOR i IN 1 TO maximum_count DO
    INSERT INTO test_for_loop_insert VALUES (:i);
    counter := counter + 1;
  END FOR;
  RETURN counter || ' rows inserted';
END;

DECLARE
  total_price FLOAT;
  c1 CURSOR FOR SELECT i FROM test_for_loop_insert;
BEGIN
  total_price := 0.0;
  FOR record IN c1 DO
    total_price := total_price + record.i;
  END FOR;
  RETURN total_price;
END;

DECLARE
  total_price FLOAT;
  rs RESULTSET;
BEGIN
  total_price := 0.0;
  rs := (SELECT i FROM test_for_loop_insert);
  FOR record IN rs DO
    total_price := total_price + record.i;
  END FOR;
  RETURN total_price;
END;

BEGIN
  LET counter := 0;
  WHILE (counter < 5) DO
    counter := counter + 1;
  END WHILE;
  RETURN counter;
END;

BEGIN
  LET counter := 5;
  LET number_of_iterations := 0;
  REPEAT
    counter := counter - 1;
    number_of_iterations := number_of_iterations + 1;
  UNTIL (counter = 0)
  END REPEAT;
  RETURN number_of_iterations;
END;

BEGIN
  LET counter := 5;
  LOOP
    IF (counter = 0) THEN
      BREAK;
    END IF;
    counter := counter - 1;
  END LOOP;
  RETURN counter;
END;

BEGIN
  LET inner_counter := 0;
  LET outer_counter := 0;
  LOOP
    LOOP
      IF (inner_counter < 5) THEN
        inner_counter := inner_counter + 1;
        CONTINUE OUTER;
      ELSE
        BREAK OUTER;
      END IF;
    END LOOP INNER;
    outer_counter := outer_counter + 1;
    BREAK;
  END LOOP OUTER;
  RETURN ARRAY_CONSTRUCT(outer_counter, inner_counter);
END;

DECLARE
  row_price FLOAT;
  total_price FLOAT;
  c1 CURSOR FOR SELECT i FROM test_for_loop_insert;
BEGIN
  row_price := 0.0;
  total_price := 0.0;
  OPEN c1;
  FETCH c1 INTO row_price;
  total_price := total_price + row_price;
  FETCH c1 INTO row_price;
  total_price := total_price + row_price;
  CLOSE c1;
  RETURN total_price;
END;

DECLARE
  total_price FLOAT;
  c1 CURSOR FOR SELECT i FROM test_for_loop_insert;
BEGIN
  total_price := 0.0;
  FOR record IN c1 DO
    total_price := total_price + record.i;
  END FOR;
  RETURN total_price;
END;


 DECLARE
  res RESULTSET;
  col_name VARCHAR;
  select_statement VARCHAR;
BEGIN
  col_name := 'i';
  select_statement := 'SELECT ' || col_name || ' FROM test_for_loop_insert';
  res := (EXECUTE IMMEDIATE select_statement);
  RETURN TABLE(res);
END;

CREATE OR REPLACE PROCEDURE test_sp_dynamic(table_name VARCHAR)
  RETURNS TABLE(a INTEGER)
  LANGUAGE SQL
AS
DECLARE
  res RESULTSET;
  query VARCHAR DEFAULT 'SELECT i FROM IDENTIFIER(?) ORDER BY i;';
BEGIN
  res := (EXECUTE IMMEDIATE :query USING(table_name));
  RETURN TABLE(res);
END;

call test_sp_dynamic('test_for_loop_insert');

CREATE OR REPLACE TABLE my_values (value NUMBER);