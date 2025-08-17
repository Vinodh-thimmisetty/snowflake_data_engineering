-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

/*

CLS - Column Level SECURITY
SDM - Static Data Masking
DDM - Dynamic Data Masking 
*/

use role sysadmin;
create or replace database dummy;
create or replace schema tmp with managed access;
CREATE OR REPLACE TRANSIENT TABLE DUMMY.TMP.CUSTOMER (
    CUSTOMER_ID NUMBER IDENTITY(1,1),
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    GENDER VARCHAR(1),
    SSN VARCHAR(11),
    DATE_OF_BIRTH DATE,
    CREDIT_CARD_NUMBER TEXT, -- with tag (TAG_NAME='TAG_VALUE', OTHER_TAG_NAME='OTHER_TAG_VALUE', and so on..)
    CREDIT_CARD_EXPIRY DATE,
    CREDIT_CARD_CVV VARCHAR(4),
    ANNUAL_INCOME NUMBER(12,2),
    MOBILE_NUMBER VARCHAR(15),
    ADDRESS_LINE1 VARCHAR(100),
    ADDRESS_LINE2 VARCHAR(100),
    CITY VARCHAR(50),
    STATE VARCHAR(2),
    ZIP_CODE VARCHAR(10),
    COUNTRY VARCHAR(50),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

INSERT INTO DUMMY.TMP.CUSTOMER (FIRST_NAME, LAST_NAME, GENDER, SSN, DATE_OF_BIRTH, CREDIT_CARD_NUMBER, CREDIT_CARD_EXPIRY, CREDIT_CARD_CVV, ANNUAL_INCOME, MOBILE_NUMBER, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTRY)
VALUES
('John', 'Smith', 'M', '123-45-6789', '1985-03-15', '4111-1111-1111-1111', '2025-12-01', '123', 75000.00, '555-123-4567', '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'USA'),
('Mary', 'Johnson', 'F', '234-56-7890', '1990-07-22', '5555-5555-5555-4444', '2024-08-01', '456', 82000.00, '555-234-5678', '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90001', 'USA'),
('Robert', 'Williams', 'M', '345-67-8901', '1978-11-30', '3782-822463-10005', '2026-03-01', '789', 95000.00, '555-345-6789', '789 Pine St', 'Suite 100', 'Chicago', 'IL', '60601', 'USA'),
('Patricia', 'Brown', 'F', '456-78-9012', '1982-04-18', '6011-1111-1111-1117', '2025-09-01', '234', 68000.00, '555-456-7890', '321 Elm St', NULL, 'Houston', 'TX', '77001', 'USA'),
('Michael', 'Davis', 'M', '567-89-0123', '1995-09-25', '3566-0020-2036-0505', '2024-11-01', '567', 71000.00, '555-567-8901', '654 Maple Dr', 'Unit 3C', 'Phoenix', 'AZ', '85001', 'USA'),
('Jennifer', 'Miller', 'F', '678-90-1234', '1988-12-03', '4111-1111-1111-1111', '2026-06-01', '890', 88000.00, '555-678-9012', '987 Cedar Ln', NULL, 'Philadelphia', 'PA', '19101', 'USA'),
('William', 'Wilson', 'M', '789-01-2345', '1992-06-11', '5555-5555-5555-4444', '2025-04-01', '345', 79000.00, '555-789-0123', '741 Birch Rd', 'Apt 7D', 'San Diego', 'CA', '92101', 'USA'),
('Elizabeth', 'Moore', 'F', '890-12-3456', '1975-08-20', '3782-822463-10005', '2024-10-01', '678', 92000.00, '555-890-1234', '852 Spruce St', NULL, 'Dallas', 'TX', '75201', 'USA'),
('David', 'Taylor', 'M', '901-23-4567', '1987-01-28', '6011-1111-1111-1117', '2026-01-01', '901', 85000.00, '555-901-2345', '963 Ash Ave', 'Suite 200', 'San Francisco', 'CA', '94101', 'USA'),
('Sarah', 'Anderson', 'F', '012-34-5678', '1993-05-07', '3566-0020-2036-0505', '2025-07-01', '432', 76000.00, '555-012-3456', '159 Walnut St', NULL, 'Seattle', 'WA', '98101', 'USA'),
('James', 'Thomas', 'M', '123-45-6780', '1980-10-14', '4111-1111-1111-1111', '2024-12-01', '765', 98000.00, '555-123-4568', '357 Cherry Ln', 'Apt 12B', 'Boston', 'MA', '02101', 'USA'),
('Lisa', 'Jackson', 'F', '234-56-7891', '1991-02-23', '5555-5555-5555-4444', '2026-05-01', '098', 72000.00, '555-234-5679', '753 Pine Ave', NULL, 'Miami', 'FL', '33101', 'USA'),
('Richard', 'White', 'M', '345-67-8902', '1983-07-09', '3782-822463-10005', '2025-02-01', '321', 81000.00, '555-345-6780', '951 Oak St', 'Unit 5E', 'Denver', 'CO', '80201', 'USA'),
('Susan', 'Harris', 'F', '456-78-9013', '1986-12-17', '6011-1111-1111-1117', '2024-09-01', '654', 69000.00, '555-456-7891', '258 Maple Ave', NULL, 'Atlanta', 'GA', '30301', 'USA'),
('Thomas', 'Martin', 'M', '567-89-0124', '1989-04-05', '3566-0020-2036-0505', '2026-08-01', '987', 93000.00, '555-567-8902', '456 Elm Dr', 'Suite 300', 'Portland', 'OR', '97201', 'USA');

grant usage on warehouse transform_wh to role sysadmin;
grant usage on warehouse transform_wh to role useradmin;
grant usage on warehouse transform_wh to role public;

grant usage on database dummy to role useradmin;
grant usage on database dummy to role public;

grant usage on schema tmp to role useradmin;
grant usage on schema tmp to role public;

grant select on table CUSTOMER to role useradmin;
grant select on table CUSTOMER to role public;


select * from DUMMY.TMP.CUSTOMER;

create or replace masking policy DUMMY.TMP.PII_GOVT_ID_MASK  as (pii_input string)
    returns string -> 
    case 
        when current_role() in ('SYSADMIN', 'ACCOUNTADMIN')
            then pii_input
        when current_role() in ('USERADMIN')
            then regexp_replace(pii_input, left(pii_input,7), 'xxx-xx-')
        else '***MASKED***'
    end;
    
show masking policies;
desc masking policy PII_GOVT_ID_MASK;
select get_ddl('POLICY', 'PII_GOVT_ID_MASK') as mask_policy;

alter table customer modify column SSN set masking policy PII_GOVT_ID_MASK;
-- alter table customer modify column SSN unset masking policy;

CREATE OR REPLACE MASKING POLICY DUMMY.TMP.PII_CREDIT_CARD_MASK AS
(val varchar) RETURNS varchar ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'USERADMIN' THEN 
            CONCAT(REPEAT('*', LENGTH(val) - 4), RIGHT(val, 4))
        ELSE REPEAT('*', 16)
    END;

alter table customer modify column CREDIT_CARD_NUMBER set masking policy PII_CREDIT_CARD_MASK;

CREATE OR REPLACE MASKING POLICY DUMMY.TMP.PII_DATE_OF_BIRTH_MASK AS
(val DATE) 
RETURNS DATE
->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    ELSE '1900-01-01'::DATE
  END;

alter table customer modify column DATE_OF_BIRTH set masking policy PII_DATE_OF_BIRTH_MASK;

ALTER TABLE DUMMY.TMP.CUSTOMER ADD COLUMN CREDIT_CARD_PROVIDER VARCHAR(50); -- BEFORE CREDIT_CARD_NUMBER;
UPDATE customer 
SET CREDIT_CARD_PROVIDER = 
    CASE 
        WHEN LEFT(CREDIT_CARD_NUMBER, 1) = '4' THEN 'VISA'
        WHEN LEFT(CREDIT_CARD_NUMBER, 1) = '5' THEN 'MASTERCARD'
        WHEN LEFT(CREDIT_CARD_NUMBER, 1) = '3' THEN 'AMEX'
        WHEN LEFT(CREDIT_CARD_NUMBER, 1) = '6' THEN 'DISCOVER'
        ELSE 'UNKNOWN'
    END;

select * from customer;

 

alter masking policy DUMMY.TMP.PII_GOVT_ID_MASK
    set body -> 
    case 
        when current_role() in ('SYSADMIN', 'ACCOUNTADMIN')
            then pii_input
        when current_role() in ('USERADMIN')
            then iff(length(pii_input) > 10, regexp_replace(pii_input, left(pii_input,7), 'xxx-xx-'), '***MASKED***')
        else '***MASKED***'
    end;

alter table customer modify column CREDIT_CARD_PROVIDER set masking policy PII_GOVT_ID_MASK;

CREATE OR REPLACE MASKING POLICY DUMMY.TMP.PII_INCOME_MASK AS
(val NUMBER) 
RETURNS NUMBER
->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
    WHEN HOUR(CURRENT_TIMESTAMP()) BETWEEN 9 AND 16 THEN val
    ELSE -1
  END;

  alter table customer modify column ANNUAL_INCOME set masking policy PII_INCOME_MASK;

  CREATE OR REPLACE MASKING POLICY dummy.tmp.PII_MOBILE_NUMBER_MASK1 AS
(val varchar) RETURNS varchar ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
        ELSE REGEXP_REPLACE(val, '\\d', 'X')
    END;

CREATE OR REPLACE MASKING POLICY dummy.tmp.PII_MOBILE_NUMBER_MASK2 AS
(val varchar) RETURNS varchar ->
    CASE
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
        ELSE '***-***-' || RIGHT(val, 4)
    END;

  alter table customer modify column MOBILE_NUMBER set masking policy PII_MOBILE_NUMBER_MASK1;
  alter table customer modify column MOBILE_NUMBER set masking policy PII_MOBILE_NUMBER_MASK2;
-- Specified column already attached to another masking policy. A column cannot be attached to multiple masking policies. Please drop the current association in order to attach a new masking policy.

  alter table customer modify column MOBILE_NUMBER unset masking policy;
 
  alter table customer modify column MOBILE_NUMBER set masking policy PII_MOBILE_NUMBER_MASK2;

CREATE OR REPLACE MASKING POLICY dummy.tmp.PII_GENDER_MASK AS
(val varchar, state varchar) 
RETURNS varchar ->
    CASE 
        WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() IN ('PUBLIC', 'USERADMIN') AND UPPER(state) IN ('NY', 'CA') THEN val
        ELSE '*******'
    END;

ALTER TABLE customer MODIFY COLUMN GENDER 
SET MASKING POLICY PII_GENDER_MASK
USING(GENDER, STATE);


-- ALTER TABLE customer MODIFY COLUMN GENDER 
-- UNSET MASKING POLICY;

CREATE OR REPLACE TAG DUMMY.TMP.PII_POLICY_TAG COMMENT = 'Tag for managing PII data policies';
CREATE OR REPLACE TAG DUMMY.TMP.PII_FINANCE_POLICY_TAG COMMENT = 'Tag for managing Financial PII data policies';
-- cannot tag multiples to single: a column allow only one masking policy

ALTER TABLE DUMMY.TMP.CUSTOMER MODIFY COLUMN SSN SET TAG PII_POLICY_TAG = 'PII';
ALTER TABLE DUMMY.TMP.CUSTOMER MODIFY COLUMN CREDIT_CARD_NUMBER SET TAG PII_POLICY_TAG = 'PII';
ALTER TABLE DUMMY.TMP.CUSTOMER MODIFY COLUMN MOBILE_NUMBER SET TAG PII_POLICY_TAG = 'PII';

ALTER TABLE DUMMY.TMP.CUSTOMER MODIFY COLUMN CREDIT_CARD_NUMBER SET TAG PII_FINANCE_POLICY_TAG = 'PII_HIGH';

CREATE OR REPLACE MASKING POLICY PII_DATA as (input_val string)
returns string -> '***PII***'::varchar;

CREATE OR REPLACE MASKING POLICY PII_FINANCE_DATA as (input_val string)
returns string -> '***PII FINANCE***'::varchar;

ALTER TAG PII_POLICY_TAG SET MASKING POLICY PII_DATA;
-- ALTER TAG PII_POLICY_TAG UNSET MASKING POLICY PII_DATA;


-- JSON data masking ( VARIANT column)

CREATE OR REPLACE MASKING POLICY PII_JSON_DATA as (input_json variant, input_key1 string, input_key2 string)
returns variant -> OBJECT_INSERT(OBJECT_INSERT(input_json, input_key1, '---MASKED---', true), input_key2, '---MASKED---', true)::variant;



-- VIEW level Policies ( same as table and will be high prioprity if any policy added on view columns)


CREATE OR REPLACE TRANSIENT TABLE DUMMY.TMP.SAMPLE_CUSTOMER (
    CUSTOMER_ID NUMBER IDENTITY(1,1),
    FIRST_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50),
    GENDER VARCHAR(1),
    SSN VARCHAR(11),
    DATE_OF_BIRTH DATE,
    CREDIT_CARD_NUMBER TEXT,
    CREDIT_CARD_EXPIRY DATE,
    CREDIT_CARD_CVV VARCHAR(4),
    ANNUAL_INCOME NUMBER(12,2),
    MOBILE_NUMBER VARCHAR(15),
    ADDRESS_LINE1 VARCHAR(100),
    ADDRESS_LINE2 VARCHAR(100),
    CITY VARCHAR(50),
    STATE VARCHAR(2),
    ZIP_CODE VARCHAR(10),
    COUNTRY VARCHAR(50),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

INSERT INTO DUMMY.TMP.SAMPLE_CUSTOMER (FIRST_NAME, LAST_NAME, GENDER, SSN, DATE_OF_BIRTH, CREDIT_CARD_NUMBER, CREDIT_CARD_EXPIRY, CREDIT_CARD_CVV, ANNUAL_INCOME, MOBILE_NUMBER, ADDRESS_LINE1, ADDRESS_LINE2, CITY, STATE, ZIP_CODE, COUNTRY)
VALUES
('John', 'Smith', 'M', '123-45-6789', '1985-03-15', '4111-1111-1111-1111', '2025-12-01', '123', 75000.00, '555-123-4567', '123 Main St', 'Apt 4B', 'New York', 'NY', '10001', 'USA'),
('Mary', 'Johnson', 'F', '234-56-7890', '1990-07-22', '5555-5555-5555-4444', '2024-08-01', '456', 82000.00, '555-234-5678', '456 Oak Ave', NULL, 'Los Angeles', 'CA', '90001', 'USA'),
('Robert', 'Williams', 'M', '345-67-8901', '1978-11-30', '3782-822463-10005', '2026-03-01', '789', 95000.00, '555-345-6789', '789 Pine St', 'Suite 100', 'Chicago', 'IL', '60601', 'USA'),
('Patricia', 'Brown', 'F', '456-78-9012', '1982-04-18', '6011-1111-1111-1117', '2025-09-01', '234', 68000.00, '555-456-7890', '321 Elm St', NULL, 'Houston', 'TX', '77001', 'USA'),
('Michael', 'Davis', 'M', '567-89-0123', '1995-09-25', '3566-0020-2036-0505', '2024-11-01', '567', 71000.00, '555-567-8901', '654 Maple Dr', 'Unit 3C', 'Phoenix', 'AZ', '85001', 'USA'),
('Jennifer', 'Miller', 'F', '678-90-1234', '1988-12-03', '4111-1111-1111-1111', '2026-06-01', '890', 88000.00, '555-678-9012', '987 Cedar Ln', NULL, 'Philadelphia', 'PA', '19101', 'USA'),
('William', 'Wilson', 'M', '789-01-2345', '1992-06-11', '5555-5555-5555-4444', '2025-04-01', '345', 79000.00, '555-789-0123', '741 Birch Rd', 'Apt 7D', 'San Diego', 'CA', '92101', 'USA'),
('Elizabeth', 'Moore', 'F', '890-12-3456', '1975-08-20', '3782-822463-10005', '2024-10-01', '678', 92000.00, '555-890-1234', '852 Spruce St', NULL, 'Dallas', 'TX', '75201', 'USA'),
('David', 'Taylor', 'M', '901-23-4567', '1987-01-28', '6011-1111-1111-1117', '2026-01-01', '901', 85000.00, '555-901-2345', '963 Ash Ave', 'Suite 200', 'San Francisco', 'CA', '94101', 'USA'),
('Sarah', 'Anderson', 'F', '012-34-5678', '1993-05-07', '3566-0020-2036-0505', '2025-07-01', '432', 76000.00, '555-012-3456', '159 Walnut St', NULL, 'Seattle', 'WA', '98101', 'USA'),
('James', 'Thomas', 'M', '123-45-6780', '1980-10-14', '4111-1111-1111-1111', '2024-12-01', '765', 98000.00, '555-123-4568', '357 Cherry Ln', 'Apt 12B', 'Boston', 'MA', '02101', 'USA'),
('Lisa', 'Jackson', 'F', '234-56-7891', '1991-02-23', '5555-5555-5555-4444', '2026-05-01', '098', 72000.00, '555-234-5679', '753 Pine Ave', NULL, 'Miami', 'FL', '33101', 'USA'),
('Richard', 'White', 'M', '345-67-8902', '1983-07-09', '3782-822463-10005', '2025-02-01', '321', 81000.00, '555-345-6780', '951 Oak St', 'Unit 5E', 'Denver', 'CO', '80201', 'USA'),
('Susan', 'Harris', 'F', '456-78-9013', '1986-12-17', '6011-1111-1111-1117', '2024-09-01', '654', 69000.00, '555-456-7891', '258 Maple Ave', NULL, 'Atlanta', 'GA', '30301', 'USA'),
('Thomas', 'Martin', 'M', '567-89-0124', '1989-04-05', '3566-0020-2036-0505', '2026-08-01', '987', 93000.00, '555-567-8902', '456 Elm Dr', 'Suite 300', 'Portland', 'OR', '97201', 'USA');


create or replace view STANDARD_customer_VW as select * from SAMPLE_CUSTOMER;
create or replace secure view SECURE_STANDARD_customer_VW as select * from SAMPLE_CUSTOMER;
create or replace materialized VIEW STANDARD_MATERIALIZED_customer_VW as select * from SAMPLE_CUSTOMER;
create or replace secure materialized VIEW SECURE_MATERIALIZED_customer_VW as select * from SAMPLE_CUSTOMER;

ALTER VIEW STANDARD_customer_VW MODIFY COLUMN  CREDIT_CARD_NUMBER SET MASKING POLICY PII_FINANCE_DATA; 
ALTER VIEW SECURE_STANDARD_customer_VW MODIFY COLUMN  CREDIT_CARD_NUMBER SET MASKING POLICY PII_FINANCE_DATA; 
ALTER VIEW STANDARD_MATERIALIZED_customer_VW MODIFY COLUMN  CREDIT_CARD_NUMBER SET MASKING POLICY PII_FINANCE_DATA; 
ALTER VIEW SECURE_MATERIALIZED_customer_VW MODIFY COLUMN  CREDIT_CARD_NUMBER SET MASKING POLICY PII_FINANCE_DATA; 