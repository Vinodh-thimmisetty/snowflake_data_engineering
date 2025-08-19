CREATE OR REPLACE TABLE orders_q1_2024 (
  order_id INT,
  order_amount NUMBER(12,2));

INSERT INTO orders_q1_2024 VALUES (1, 500.00);
INSERT INTO orders_q1_2024 VALUES (2, 225.00);
INSERT INTO orders_q1_2024 VALUES (3, 725.00);
INSERT INTO orders_q1_2024 VALUES (4, 150.00);
INSERT INTO orders_q1_2024 VALUES (5, 900.00);

CREATE OR REPLACE TABLE orders_q2_2024 (
  order_id INT,
  order_amount NUMBER(12,2));

INSERT INTO orders_q2_2024 VALUES (1, 100.00);
INSERT INTO orders_q2_2024 VALUES (2, 645.00);
INSERT INTO orders_q2_2024 VALUES (3, 275.00);
INSERT INTO orders_q2_2024 VALUES (4, 800.00);
INSERT INTO orders_q2_2024 VALUES (5, 250.00);

CREATE OR REPLACE PROCEDURE test_sp_async_child_jobs_query()
RETURNS INTEGER
LANGUAGE SQL
AS
DECLARE
  accumulator1 INTEGER DEFAULT 0;
  accumulator2 INTEGER DEFAULT 0;
  res1 RESULTSET DEFAULT ASYNC (SELECT order_amount FROM orders_q1_2024);
  res2 RESULTSET DEFAULT ASYNC (SELECT order_amount FROM orders_q2_2024);
BEGIN
  AWAIT res1;
  LET cur1 CURSOR FOR res1;
  OPEN cur1;
  AWAIT res2;
  LET cur2 CURSOR FOR res2;
  OPEN cur2;
  FOR row_variable IN cur1 DO
      accumulator1 := accumulator1 + row_variable.order_amount;
  END FOR;
  FOR row_variable IN cur2 DO
      accumulator2 := accumulator2 + row_variable.order_amount;
  END FOR;
  RETURN accumulator1 + accumulator2;
END;

call test_sp_async_child_jobs_query();


CREATE OR REPLACE PROCEDURE test_sp_async_child_jobs_insert(
  arg1 INT,
  arg2 NUMBER(12,2),
  arg3 INT,
  arg4 NUMBER(12,2))
RETURNS TABLE()
LANGUAGE SQL
AS
  BEGIN
   CREATE TABLE IF NOT EXISTS orders_q3_2024 (
      order_id INT,
      order_amount NUMBER(12,2));
    LET insert_1 RESULTSET := ASYNC (INSERT INTO orders_q3_2024 SELECT :arg1, :arg2);
    LET insert_2 RESULTSET := ASYNC (INSERT INTO orders_q3_2024 SELECT :arg3, :arg4);
    AWAIT insert_1;
    AWAIT insert_2;
    LET res RESULTSET := (SELECT * FROM orders_q3_2024 ORDER BY order_id);
    RETURN TABLE(res);
  END;

call test_sp_async_child_jobs_insert(1, 10000, 2, 20000);


CREATE OR REPLACE PROCEDURE test_async_child_job_inserts()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  CREATE OR REPLACE TABLE test_child_job_queries1 (col1 INT);
  ASYNC (INSERT INTO test_child_job_queries1(col1) VALUES(1));
  ASYNC (INSERT INTO test_child_job_queries1(col1) VALUES(2));
  ASYNC (INSERT INTO test_child_job_queries1(col1) VALUES(3));
  AWAIT ALL;
END;
$$
;

CREATE OR REPLACE TABLE test_child_job_queries2 (id INT, cola INT);

INSERT INTO test_child_job_queries2 VALUES
  (1, 100), (2, 101), (3, 102);

CREATE OR REPLACE PROCEDURE test_async_child_job_updates()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
  ASYNC (UPDATE test_child_job_queries2 SET cola=200 WHERE id=1);
  ASYNC (UPDATE test_child_job_queries2 SET cola=201 WHERE id=2);
  ASYNC (UPDATE test_child_job_queries2 SET cola=202 WHERE id=3);
  AWAIT ALL;
END;

CREATE OR REPLACE PROCEDURE test_async_child_job_calls()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
  ASYNC (CALL test_async_child_job_inserts());
  ASYNC (CALL test_async_child_job_updates());
  AWAIT ALL;
END;

call test_async_child_job_calls();

SELECT col1 FROM test_child_job_queries1 ORDER BY col1;