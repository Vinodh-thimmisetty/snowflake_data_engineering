-- ENABLE AI 
use role accountadmin;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US';
SHOW PARAMETERS LIKE 'CORTEX_ENABLED_CROSS_REGION' IN ACCOUNT;

-- Create a Github Secret with PAT Classic Token
CREATE OR REPLACE SECRET github_secret
  TYPE = password
  USERNAME = '**********'
  PASSWORD = '**********';

-- Create a Gitub Integration 
CREATE OR REPLACE API INTEGRATION github_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/Vinodh-thimmisetty/')
  allowed_authentication_secrets = (github_secret)
  ENABLED = true;

-- Link the Github Integration to Workspaces.

