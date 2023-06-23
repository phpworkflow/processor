<?php

namespace PhpWorkflow\Processor;

class Config
{
    /**
     * @return string
     */
    public function getDSN(): string
    {
        $dsn = getenv('WORKFLOW_DB_DSN');

        return $dsn ?: ($_ENV['WORKFLOW_DB_DSN'] ?? '');
    }

    /**
     * Path to named pipe for exchange
     * @return string
     */
    public function getWorkflowExchangePipeName(): string
    {
        $env = getenv('WORKFLOW_EXCHANGE_PIPE_NAME');

        return $env ?: ($_ENV['WORKFLOW_EXCHANGE_PIPE_NAME'] ?? '/tmp/workflow_exchange_pipe');
    }

    /**
     * Configuration for certain workflows
     *  { WorkflowClass1 => numWorkers1,  WorkflowClass2 => numWorkers2}
     * @return array
     */
    public function getJobsPerWorkerCfg(): array {
        $env = getenv('WORKFLOW_JOBS_PER_WORKER');

        $env = $env ?: ($_ENV['WORKFLOW_JOBS_PER_WORKER'] ?? '[]');
        return json_decode($env, true) ?: [];
    }

    public function getNumberOfWorkers(): int {
        $env = getenv('WORKFLOW_NUMBER_OF_WORKERS');

        return $env ?: ($_ENV['WORKFLOW_NUMBER_OF_WORKERS'] ?? 5);

    }
}