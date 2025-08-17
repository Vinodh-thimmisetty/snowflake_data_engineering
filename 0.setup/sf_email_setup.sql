-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

CREATE OR REPLACE NOTIFICATION INTEGRATION notification
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('vinodhkumar5052@gmail.com', 'other_email@gmail.com');

CALL SYSTEM$SEND_EMAIL(
    'notification',
    'vinodhkumar5052@gmail.com',
    'New Snowflake Session Alert',
    'A new session was created in your Snowflake account.'
  );

CREATE OR REPLACE ALERT WAREHOUSE_USAGE_ALERT
WAREHOUSE = 'TRANSFORM_WH'
SCHEDULE = '1440 MINUTE'
IF (EXISTS (
    SELECT SUM(CREDITS_USED) * 4 as daily_cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('day', START_TIME)
    HAVING daily_cost > 50
))
THEN
    CALL SYSTEM$SEND_EMAIL(
        'notification',
        'vinodhkumar5052@gmail.com',
        'High Warehouse Usage Alert',
        'Daily warehouse usage cost has exceeded $50'
    ); 