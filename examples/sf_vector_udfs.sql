-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;


CREATE OR REPLACE FUNCTION add_numbers(a FLOAT, b FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'add_numbers'
AS $$
def add_numbers(a, b):
    return a + b
$$;

 
select O_ORDERKEY, O_TOTALPRICE, add_numbers(O_TOTALPRICE, -0.1*O_TOTALPRICE) as FINAL_PRICE
from snowflake_sample_data.tpch_sf1000.orders limit 100;
 

CREATE FUNCTION add_numbers_vector(x NUMBER(10, 0), y NUMBER(10, 0))
  RETURNS NUMBER(10, 0)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.10
  PACKAGES = ('pandas')
  HANDLER = 'add_numbers_vector'
AS $$
import pandas
from _snowflake import vectorized

@vectorized(input=pandas.DataFrame)
def add_numbers_vector(df):
 return df[0] + df[1] 
$$;

select O_ORDERKEY, O_TOTALPRICE, add_numbers_vector(O_TOTALPRICE, -0.1*O_TOTALPRICE) as FINAL_PRICE
from snowflake_sample_data.tpch_sf1000.orders limit 10000;


CREATE FUNCTION add_numbers_vector_modin(x NUMBER(10, 0), y NUMBER(10, 0))
  RETURNS NUMBER(10, 0)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.10
  PACKAGES = ('modin', 'typing_extensions', 'snowflake-snowpark-python')
  HANDLER = 'add_numbers_vector_modin'
AS $$
import modin.pandas as pd
import snowflake.snowpark.modin.plugin
from _snowflake import vectorized

@vectorized(input=pd.DataFrame)
def add_numbers_vector_modin(df):
 return df[0] + df[1] 
$$;

select O_ORDERKEY, O_TOTALPRICE, add_numbers_vector_modin(O_TOTALPRICE, -0.1*O_TOTALPRICE) as FINAL_PRICE
from snowflake_sample_data.tpch_sf1000.orders limit 10000;


CREATE OR REPLACE TRANSIENT TABLE test_values(id VARCHAR, col1 FLOAT, col2 FLOAT, col3 FLOAT, col4 FLOAT, col5 FLOAT);

-- generate 3 partitions of 5 rows each
INSERT INTO test_values
  SELECT 'x',
  UNIFORM(1.5,1000.5,RANDOM(1))::FLOAT col1,
  UNIFORM(1.5,1000.5,RANDOM(2))::FLOAT col2,
  UNIFORM(1.5,1000.5,RANDOM(3))::FLOAT col3,
  UNIFORM(1.5,1000.5,RANDOM(4))::FLOAT col4,
  UNIFORM(1.5,1000.5,RANDOM(5))::FLOAT col5
  FROM TABLE(GENERATOR(ROWCOUNT => 5));

INSERT INTO test_values
  SELECT 'y',
  UNIFORM(1.5,1000.5,RANDOM(10))::FLOAT col1,
  UNIFORM(1.5,1000.5,RANDOM(20))::FLOAT col2,
  UNIFORM(1.5,1000.5,RANDOM(30))::FLOAT col3,
  UNIFORM(1.5,1000.5,RANDOM(40))::FLOAT col4,
  UNIFORM(1.5,1000.5,RANDOM(50))::FLOAT col5
  FROM TABLE(GENERATOR(ROWCOUNT => 5));

INSERT INTO test_values
  SELECT 'z',
  UNIFORM(1.5,1000.5,RANDOM(100))::FLOAT col1,
  UNIFORM(1.5,1000.5,RANDOM(200))::FLOAT col2,
  UNIFORM(1.5,1000.5,RANDOM(300))::FLOAT col3,
  UNIFORM(1.5,1000.5,RANDOM(400))::FLOAT col4,
  UNIFORM(1.5,1000.5,RANDOM(500))::FLOAT col5
  FROM TABLE(GENERATOR(ROWCOUNT => 5));

 SELECT * FROM test_values;

 
CREATE OR REPLACE FUNCTION summary_stats(id VARCHAR, col1 FLOAT, col2 FLOAT, col3 FLOAT, col4 FLOAT, col5 FLOAT)
  RETURNS TABLE (column_name VARCHAR, count INT, mean FLOAT, std FLOAT, min FLOAT, q1 FLOAT, median FLOAT, q3 FLOAT, max FLOAT)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.9
  PACKAGES = ('pandas')
  HANDLER = 'handler'
AS $$
from _snowflake import vectorized
import pandas

class handler:
    @vectorized(input=pandas.DataFrame)
    def end_partition(self, df):
      # using describe function to get the summary statistics
      result = df.describe().transpose()
      # add a column at the beginning for column ids
      result.insert(loc=0, column='column_name', value=['col1', 'col2', 'col3', 'col4', 'col5'])
      return result
$$;
 
CREATE OR REPLACE FUNCTION summary_stats(id VARCHAR, col1 FLOAT, col2 FLOAT, col3 FLOAT, col4 FLOAT, col5 FLOAT)
  RETURNS TABLE (column_name VARCHAR, count INT, mean FLOAT, std FLOAT, min FLOAT, q1 FLOAT, median FLOAT, q3 FLOAT, max FLOAT)
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.9
  PACKAGES = ('pandas')
  HANDLER = 'handler'
AS $$
from _snowflake import vectorized
import pandas

 
$$;

-- partition by id
SELECT * FROM test_values, TABLE(summary_stats(id, col1, col2, col3, col4, col5)
  OVER (PARTITION BY id))
  ORDER BY id, column_name;

