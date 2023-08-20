# Workflow processor
Processor for execution phpworkflows 

## Environment variables
**WORKFLOW_DB_DSN** - database connection string. **Required.** Example: pgsql:user=dbuser;password=Pass;host=localhost;port=5432;dbname=workflow_db

**WORKFLOW_LOG_CHANNEL** - log channel for workflow logger. Value of Workflow\ILogger::LOG_* constants. 0 by default.

**WORKFLOW_WORKFLOWS_PER_REQUEST** - max number of workflows per database request. 100 by default.

**WORKFLOW_NUMBER_OF_WORKERS** - number of worker processes. 5 by default.

**WORKFLOW_EXCHANGE_PIPE_NAME** - named pipe for exchange between main process and supplier process. /tmp/workflow_exchange_pipe by default.

**WORKFLOW_JOBS_PER_WORKER** - number of workflows per worker type. 5 by default. 
Example: WORKFLOW_JOBS_PER_WORKER={"Workflow\\Example\\GoodsSaleWorkflow":3,"Workflow\\Example\\CommandsQueue":1}

## Usage
1. Create postgres database for workflows. Use structure from phpworkflow/docs/postgres.sql.
2. Setup environment variables.
3. Create your own script like scripts in **bin** folder. 