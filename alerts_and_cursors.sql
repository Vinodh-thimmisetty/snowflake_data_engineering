EXECUTE IMMEDIATE
$$
DECLARE
    i INTEGER;
    j INTEGER;
    pattern TEXT DEFAULT '';
    c CURSOR for SELECT SEQ4() AS C_ID, UUID_STRING() AS HASHID FROM TABLE(GENERATOR(ROWCOUNT=>(?::NUMBER)));
    rs RESULTSET DEFAULT (SELECT SEQ4() AS C_ID, UUID_STRING() AS HASHID FROM TABLE(GENERATOR(ROWCOUNT=>10)));
BEGIN

    FOR i IN 1 TO 5 
    DO
        FOR j IN 1 TO i 
        DO
            pattern := pattern || '*\t';
        END FOR;
        pattern := pattern || '\n';
    END FOR; 

    -- RETURN pattern;

    LET N NUMBER := 13;
    LET S NUMBER DEFAULT 2;
    WHILE (S<N)
    DO
        IF(N%S=0) THEN
            pattern := pattern || 'Prime';
            BREAK;
        ELSE
            S := S + 1;
        END IF;
    END WHILE;

   
    OPEN c USING(:N); 
    -- FETCH c into :pattern; 
    FOR tmp in c
    do  
        pattern :=  tmp.c_ID;
    end for;     
    CLOSE c;

    RETURN TABLE(rs);


END;
$$;

SELECT SEQ4() AS C_ID, UUID_STRING() AS HASHID FROM TABLE(GENERATOR(ROWCOUNT=>10));




CREATE OR REPLACE TRANSIENT TABLE ERROR_LOG (
    ERROR_ID NUMBER AUTOINCREMENT,
    ERROR_JOB VARCHAR,
    ERROR_TYPE VARCHAR,
    ERROR_STATE VARCHAR,
    ERROR_CODE NUMBER,
    ERROR_MESSAGE VARCHAR,
    ERROR_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    ERROR_USER VARCHAR DEFAULT CURRENT_USER(),
    ERROR_SESSION_ID VARCHAR DEFAULT CURRENT_SESSION()
    -- ERROR_QUERY_ID VARCHAR DEFAULT CURRENT_QU(), -- check if it is possible
);

 SELECT * FROM DUMMY.TMP.ERROR_LOG;
 
-- EXECUTE IMMEDIATE 
-- $$
DECLARE
    NO_DATA_EXCEPTION EXCEPTION (-20001, 'No Data in Table');
    emp_id NUMBER DEFAULT 0;
BEGIN

    IF(emp_id < 1) THEN RAISE NO_DATA_EXCEPTION; END IF;

    EXCEPTION
        WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
            -- RETURN OBJECT_CONSTRUCT(
            --     'ERROR_TYPE', 'STATEMENT_OR_EXPRESSION_ERROR',
            --     'ERROR_STATE', SQLSTATE,
            --     'ERROR_CODE', SQLCODE,
            --     'ERROR_MESSAGE', SQLERRM
            -- );
            INSERT INTO DUMMY.TMP.ERROR_LOG (ERROR_JOB, ERROR_TYPE, ERROR_STATE, ERROR_CODE, ERROR_MESSAGE)
VALUES ('ANONYMOUS BLOCK', 'STATEMENT_OR_EXPRESSION_ERROR', :SQLSTATE, :SQLCODE, :SQLERRM);
            
        WHEN NO_DATA_EXCEPTION THEN
            -- RETURN OBJECT_CONSTRUCT(
            --     'ERROR_TYPE', 'NO_DATA_EXCEPTION',
            --     'ERROR_STATE', SQLSTATE,
            --     'ERROR_CODE', SQLCODE,
            --     'ERROR_MESSAGE', SQLERRM
            -- );
        INSERT INTO DUMMY.TMP.ERROR_LOG (ERROR_JOB, ERROR_TYPE, ERROR_STATE, ERROR_CODE, ERROR_MESSAGE)
VALUES ('ANONYMOUS BLOCK','NO_DATA_EXCEPTION', :SQLSTATE, :SQLCODE, :SQLERRM);
        WHEN OTHER THEN
            -- RETURN OBJECT_CONSTRUCT(
            --     'ERROR_TYPE', 'OTHER',
            --     'ERROR_STATE', SQLSTATE,
            --     'ERROR_CODE', SQLCODE,
            --     'ERROR_MESSAGE', SQLERRM
            -- );
            INSERT INTO DUMMY.TMP.ERROR_LOG (ERROR_JOB, ERROR_TYPE, ERROR_STATE, ERROR_CODE, ERROR_MESSAGE)
VALUES ('ANONYMOUS BLOCK','OTHER', :SQLSTATE, :SQLCODE, :SQLERRM);
END;
-- $$;

 
CREATE OR REPLACE SECRET test_teams_secret
  TYPE = GENERIC_STRING
  SECRET_STRING = 'abc/123/asd';


CREATE OR REPLACE NOTIFICATION INTEGRATION test_teams_webhook_integration
  TYPE=WEBHOOK
  ENABLED=TRUE
  WEBHOOK_URL='https://******/IncomingWebhook/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET=dummy.tmp.test_teams_secret
  WEBHOOK_BODY_TEMPLATE='{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
  WEBHOOK_HEADERS=('Content-Type'='application/json');



CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
  SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
    SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('This is a test Teams Alert from my Snowflake Account')
  ),
  SNOWFLAKE.NOTIFICATION.INTEGRATION('test_teams_webhook_integration')
);




CREATE or replace TRANSIENT TABLE usage_spike_alerts (
    alert_id INT AUTOINCREMENT PRIMARY KEY,
    warehouse_name STRING NOT NULL,
    last_hour_credits FLOAT NOT NULL,
    avg_monthly_credits FLOAT NOT NULL,
    credit_diff FLOAT NOT NULL,
    percent_increase FLOAT NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    alert_sent boolean default false
);


CREATE OR REPLACE TASK monitor_warehouse_spikes
SCHEDULE = 'USING CRON 2 * * * * America/New_York'
SERVERLESS_TASK_MIN_STATEMENT_SIZE = 'XSMALL'
SERVERLESS_TASK_MAX_STATEMENT_SIZE = 'XSMALL'
as
insert into usage_spike_alerts (warehouse_name,last_hour_credits,avg_monthly_credits,credit_diff,percent_increase)
WITH last_hour_usage AS (
    SELECT
        warehouse_name,
        sum(credits_used) AS last_hour_credits
    FROM
        snowflake.account_usage.warehouse_metering_history
    WHERE
        start_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP) 
        AND end_time <= CURRENT_TIMESTAMP
    GROUP BY
        warehouse_name
),
monthly_avg_usage AS (
    SELECT
        warehouse_name,
        AVG(credits_used) AS avg_monthly_credits
    FROM
        snowflake.account_usage.warehouse_metering_history
    WHERE
        start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP)
        AND start_time < DATEADD(hour, -1, CURRENT_TIMESTAMP) -- Exclude last hour
    GROUP BY
        warehouse_name
),
spikes AS (
    SELECT
        l.warehouse_name,
        l.last_hour_credits,
        m.avg_monthly_credits,
        l.last_hour_credits - m.avg_monthly_credits AS credit_diff,
        ROUND((l.last_hour_credits / NULLIF(m.avg_monthly_credits, 0) - 1) * 100, 2) AS percent_increase
    FROM
        last_hour_usage l
    INNER JOIN
        monthly_avg_usage m
    ON
        l.warehouse_name = m.warehouse_name
    WHERE
        l.last_hour_credits > m.avg_monthly_credits * 1.5 -- Customize spike threshold (e.g., 50% higher)
)
SELECT
    warehouse_name,
    last_hour_credits,
    avg_monthly_credits,
    credit_diff,
    percent_increase
FROM
    spikes
union select 'dummy_row', 0,0,0,0 -- for example purposes to ensure at least 1 row always comes to test the alert
ORDER BY
    percent_increase DESC
    ;
    
show tasks;

-- don't forget to enable your task!
alter task monitor_warehouse_spikes resume;

execute task monitor_warehouse_spikes;
select * from usage_spike_alerts where not alert_sent;


CREATE OR REPLACE PROCEDURE send_usage_spike_alerts()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_alerts'
AS $$
import snowflake.snowpark as snowpark

def send_alerts(session):
    query = """
        SELECT warehouse_name, last_hour_credits, avg_monthly_credits, credit_diff, percent_increase
        FROM usage_spike_alerts
        WHERE alert_sent = FALSE
    """
    results = session.sql(query).collect()

    if not results:
        return "No alerts to send."

    alerts_sent = 0
    for row in results:
        try:
            # Construct the single-line message
            message_content = (
                f"🚨 *Warehouse Spike Detected:* "
                f"*Warehouse*: {row['WAREHOUSE_NAME']}, "
                f"*Last Hour Credits*: {row['LAST_HOUR_CREDITS']:.4f}, "
                f"*1 Month Avg Hourly Credit,*: {row['AVG_MONTHLY_CREDITS']:.4f}, "
                f"*Difference*: {row['CREDIT_DIFF']:.4f}, "
                f"*Percent Increase*: {row['PERCENT_INCREASE']:.2f}%"
            )
            
            # Sanitize the message
            sanitized_message_query = f"""
                SELECT SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT('{message_content}')
            """
            sanitized_message = session.sql(sanitized_message_query).collect()[0][0]

            # Send the alert
            notification_query = f"""
                CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
                    SNOWFLAKE.NOTIFICATION.TEXT_PLAIN('{sanitized_message}'),
                    SNOWFLAKE.NOTIFICATION.INTEGRATION('test_teams_webhook_integration')
                )
            """
            session.sql(notification_query).collect()

            # Mark the alert as sent
            update_query = f"""
                UPDATE usage_spike_alerts
                SET alert_sent = TRUE
                WHERE warehouse_name = '{row['WAREHOUSE_NAME']}'
                  AND last_hour_credits = {row['LAST_HOUR_CREDITS']}
                  AND avg_monthly_credits = {row['AVG_MONTHLY_CREDITS']}
            """
            session.sql(update_query).collect()

            alerts_sent += 1
        except Exception as e:
            session.add_log(f"Error sending alert for warehouse {row['WAREHOUSE_NAME']}: {str(e)}")
            continue

    return f"{alerts_sent} alert(s) sent."
$$;


execute task monitor_warehouse_spikes;  -- if you haven't already...
select * from usage_spike_alerts where not alert_sent; -- review it, sent = false
CALL send_usage_spike_alerts(); -- send the alert
-- wait for the alert to come
select * from usage_spike_alerts where not alert_sent; -- 0 rows  
