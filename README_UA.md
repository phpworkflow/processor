# Процесор робочих процесів
Процесор для виконання робочих процесів бібліотеки phpworkflow

## Змінні середовища
**WORKFLOW_DB_DSN** - підключення до бази даних. **Обов'язково.** Приклад: pgsql:user=dbuser;password=Pass;host=localhost;port=5432;dbname=workflow_db

**WORKFLOW_LOG_CHANNEL** - канал журналу для робочого процесу. Значення констант Workflow\ILogger::LOG_*. За замовчуванням LOG_OFF.

**WORKFLOW_WORKFLOWS_PER_REQUEST** - максимальна кількість процесів для обробки за один запит до бази даних. За замовчуванням 100.

**WORKFLOW_NUMBER_OF_WORKERS** - кількість unix процесів-виконавців. За замовчуванням 5.

**WORKFLOW_EXCHANGE_PIPE_NAME** - назва іменованого каналу (unix named pipe ) для обміну між основним процесом та процесом-постачальником. За замовчуванням /tmp/workflow_exchange_pipe.

**WORKFLOW_JOBS_PER_WORKER** - кількість робочих процесів для типу робітника. За замовчуванням 5.
Приклад: WORKFLOW_JOBS_PER_WORKER={"Workflow\\Example\\GoodsSaleWorkflow":3,"Workflow\\Example\\CommandsQueue":1}

## Використання
1. Створіть базу даних postgres для робочих процесів. Використовуйте структуру з phpworkflow/docs/postgres.sql.
2. Налаштуйте змінні середовища.
3. Створіть свій власний скрипт, подібний до скриптів у папці **bin**.
