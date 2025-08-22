

CREATE OR REPLACE PROCEDURE last_n_query_duration(last_n NUMBER, total NUMBER)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.10
  PACKAGES=('snowflake-snowpark-python')
  HANDLER='main'
AS $$
import snowflake.snowpark.functions as funcs

def main(session, last_n, total):
  # create sample dataset to emulate id + elapsed time
  session.sql('''CREATE OR REPLACE TRANSIENT TABLE sample_query_history (query_id INT, elapsed_time FLOAT)''').collect()
  session.sql(''' INSERT INTO sample_query_history SELECT  seq8() AS query_id, uniform(0::float, 100::float, random()) as elapsed_time FROM table(generator(rowCount => {0}));'''.format(total)).collect()
  # get the mean of the last n query elapsed time
  df = session.table('sample_query_history').select( funcs.col('query_id'), funcs.col('elapsed_time')).limit(last_n)
  pandas_df = df.to_pandas()
  mean_time = pandas_df.loc[:, 'ELAPSED_TIME'].mean()
  del pandas_df
  return mean_time
$$;

CREATE OR REPLACE STAGE DUMMY.TMP.SF_PROC_PROFILING DIRECTORY = (ENABLE = TRUE);

SHOW PARAMETERS LIKE 'PYTHON_PROFILER_TARGET_STAGE';
SHOW PARAMETERS LIKE 'ACTIVE_PYTHON_PROFILER';
SHOW PARAMETERS LIKE 'PYTHON_PROFILER_MODULES';

ALTER SESSION SET PYTHON_PROFILER_TARGET_STAGE = "DUMMY.TMP.SF_PROC_PROFILING";
ALTER SESSION SET ACTIVE_PYTHON_PROFILER = 'LINE';
ALTER SESSION SET ACTIVE_PYTHON_PROFILER = 'MEMORY';

-- Sample 1 million from 10 million records
CALL last_n_query_duration(1000000, 10000000);

SELECT 
    SNOWFLAKE.CORE.GET_PYTHON_PROFILER_OUTPUT('01be8784-0000-2cf0-0048-710b00033052') as line_profiler,
    SNOWFLAKE.CORE.GET_PYTHON_PROFILER_OUTPUT('01be8785-0000-2cb2-0048-710b0002beb2') as memory_profiler;



CREATE OR REPLACE FUNCTION TEXT_QUALITY_SCORE(TEXT STRING)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'udf_code.text_quality_score'
IMPORTS = ('@DUMMY.TMP.SF_PROC_PROFILING/udf_examples/udf_code.py')
PACKAGES = ()
COMMENT = 'Sample text quality heuristic UDF';

SELECT TEXT,
       TEXT_QUALITY_SCORE(TEXT) AS SCORE
FROM (SELECT * FROM VALUES ('Hello World'), (''), ('This is GREAT!!!'), (NULL)) s(TEXT);











    