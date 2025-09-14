-- Table to hold incoming raw event data, often as semi-structured VARIANT
CREATE OR REPLACE TRANSIENT TABLE event_stream (
    event_id INT,
    event_data VARIANT,
    ingestion_time TIMESTAMP_NTZ
);

-- Audit table to log all procedure runs and store execution parameters as JSON
CREATE OR REPLACE TRANSIENT TABLE audit_log (
    run_id INT,
    status VARCHAR,
    message VARCHAR,
    parameters VARIANT,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- Table to store aggregated and processed event data
CREATE OR REPLACE TRANSIENT TABLE processed_events (
    run_id INT,
    device_id VARCHAR,
    metric_name VARCHAR,
    metric_value FLOAT,
    event_counts OBJECT,
    processing_date DATE
);

-- Sequence to generate unique IDs for the audit log
CREATE OR REPLACE SEQUENCE audit_run_id_seq START WITH 1 INCREMENT BY 1;



CREATE OR REPLACE PROCEDURE process_and_report_events(event_source_table VARCHAR, process_days INT)
RETURNS VARCHAR
LANGUAGE SQL
AS
DECLARE
    -- General variables
    run_id INT := (SELECT dummy.tmp.AUDIT_RUN_ID_SEQ.NEXTVAL);
    status VARCHAR;
    error_message VARCHAR;
    start_timestamp TIMESTAMP := CURRENT_TIMESTAMP();
    end_timestamp TIMESTAMP;
    
    -- Variables for data processing and different data types
    device_metrics_arr ARRAY;
    num_metrics INT;
    temp_obj OBJECT;
    metric_value FLOAT;
    metric_name VARCHAR;
    is_alert_needed BOOLEAN;

    -- Variables for RESULTSET, CURSOR, and loops
    events_rs RESULTSET;
    events_cursor CURSOR FOR events_rs;
    retry_counter INT := 0;

    -- Custom exception
    PROCESSING_FAILED EXCEPTION (-20002, 'Processing logic failed after retries.');

BEGIN
    -- Log the start of the process with a VARIANT object for parameters
    INSERT INTO audit_log (run_id, status, message, parameters, start_time)
    SELECT :run_id, 'STARTED', 'Procedure started for events processing.', OBJECT_CONSTRUCT('event_source_table', :event_source_table, 'process_days', :process_days), :start_timestamp;

    -- Usecase 1: Dynamic SQL to retrieve semi-structured event data
    -- Retrieve all events from the last `process_days` from a dynamic table
    LET sql_get_events VARCHAR := 'SELECT event_data FROM IDENTIFIER(?) WHERE ingestion_time >= DATEADD(day, -?, CURRENT_TIMESTAMP())';
    events_rs := (EXECUTE IMMEDIATE :sql_get_events USING (:event_source_table, :process_days));

    -- Usecase 2: Iterating with a CURSOR and processing semi-structured data
    FOR event_row IN events_cursor DO
        -- Extract values from the VARIANT column
        LET device_id VARCHAR := event_row.event_data:device_id::VARCHAR;
        LET metrics_array ARRAY := event_row.event_data:metrics::ARRAY;
        LET timestamp_str VARCHAR := event_row.event_data:timestamp::VARCHAR;
        
        -- Use built-in function to check if metrics are available
        IF (ARRAY_SIZE(metrics_array) > 0) THEN
            num_metrics := ARRAY_SIZE(metrics_array);
            
            -- Usecase 3: Using GET() to iterate an ARRAY within a loop
            -- Here, a counter-based loop is used for the inner array
            FOR i IN 0 TO num_metrics - 1 DO
                -- Get the i-th element from the metrics array
                temp_obj := GET(metrics_array, i);

                -- Extract nested values using dot notation on the OBJECT
                metric_name := temp_obj:name::VARCHAR;
                metric_value := temp_obj:value::FLOAT;
                
                -- Usecase 4: Conditional logic with CASE statement on a calculated value
                is_alert_needed := CASE
                                    WHEN metric_name = 'temperature' AND metric_value > 90.0 THEN TRUE
                                    WHEN metric_name = 'humidity' AND metric_value > 80.0 THEN TRUE
                                    ELSE FALSE
                                 END;

                IF (is_alert_needed) THEN
                    -- A simple logging scenario for demonstration
                    INSERT INTO audit_log (run_id, status, message, parameters, start_time)
                    SELECT :run_id, 'ALERT', 'High metric value detected.', OBJECT_CONSTRUCT('device_id', :device_id, 'metric', :metric_name, 'value', :metric_value), CURRENT_TIMESTAMP();
                END IF;

                -- Insert the processed data
                INSERT INTO processed_events (run_id, device_id, metric_name, metric_value, event_counts, processing_date)
                SELECT :run_id, :device_id, :metric_name, :metric_value, OBJECT_CONSTRUCT('events', 1), TO_DATE(:timestamp_str);
            END FOR;
        END IF;
    END FOR;

    -- Usecase 5: WHILE loop with a retry mechanism
    -- A practical use case for a WHILE loop is to retry an operation that might fail
    WHILE (TRUE) DO
        LET total_events_processed INT := (SELECT COUNT(*) FROM processed_events WHERE run_id = :run_id);
        
        IF (:total_events_processed > 0) THEN
            status := 'COMPLETED';
            BREAK; -- Exit loop on success
        ELSEIF (retry_counter >= 3) THEN
            RAISE PROCESSING_FAILED; -- Raise custom exception
        ELSE
            retry_counter := retry_counter + 1;
            LET retry_delay_ms INT := POW(2, :retry_counter) * 1000; -- Exponential backoff
            CALL SYSTEM$WAIT(:retry_delay_ms); -- Use an external function (SYSTEM$WAIT)
        END IF;
    END WHILE;
    
    -- Final audit log update on success
    end_timestamp := CURRENT_TIMESTAMP();
    UPDATE audit_log
    SET status = :status, message = 'Processing completed successfully.', end_time = :end_timestamp
    WHERE run_id = :run_id;

    RETURN 'Procedure finished with status: ' || :status;

EXCEPTION
    WHEN PROCESSING_FAILED THEN
        status := 'FAILED';
        error_message := SQLERRM;
        end_timestamp := CURRENT_TIMESTAMP();
        UPDATE audit_log
        SET status = :status, message = :error_message, end_time = :end_timestamp
        WHERE run_id = :run_id;
        RETURN 'Procedure finished with status: FAILED due to processing logic failure.';
    WHEN STATEMENT_ERROR THEN
        status := 'FAILED';
        error_message := 'Statement Error: ' || SQLERRM;
        end_timestamp := CURRENT_TIMESTAMP();
        UPDATE audit_log
        SET status = :status, message = :error_message, end_time = :end_timestamp
        WHERE run_id = :run_id;
        RETURN 'Procedure finished with status: FAILED due to statement error.';
    WHEN OTHER THEN
        status := 'FAILED';
        error_message := 'Other Error: ' || SQLERRM;
        end_timestamp := CURRENT_TIMESTAMP();
        UPDATE audit_log
        SET status = :status, message = :error_message, end_time = :end_timestamp
        WHERE run_id = :run_id;
        RETURN 'Procedure finished with status: FAILED due to an unexpected error.';

END;


-- Insert some raw event data
INSERT INTO DUMMY.TMP.event_stream (event_id, event_data, ingestion_time)
SELECT column1, PARSE_JSON(column2), column3 FROM VALUES
(1, '{ "device_id": "device_A", "timestamp": "2025-09-08", "metrics": [ {"name": "temperature", "value": 95.5}, {"name": "humidity", "value": 75.2} ] }', '2025-09-08 10:00:00'),
(2, '{ "device_id": "device_B", "timestamp": "2025-09-08", "metrics": [ {"name": "temperature", "value": 85.0} ] }', '2025-09-08 10:05:00'),
(3, '{ "device_id": "device_C", "timestamp": "2025-09-08", "metrics": [ {"name": "humidity", "value": 85.1} ] }', '2025-09-08 10:10:00'),
(4, '{ "device_id": "device_D", "timestamp": "2025-09-07", "metrics": [] }', '2025-09-07 09:30:00');


-- Call the procedure to process events from the last 2 days
CALL process_and_report_events('event_stream', 2);
