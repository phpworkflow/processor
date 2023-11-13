<?php

namespace PhpWorkflow\Processor;
use Workflow\Logger\ILogger;

class Config
{
    private const WORKFLOWS_PER_REQUEST = 100;

    private const NUMBER_OR_WORKERS = 5;

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

        return $env ?: ($_ENV['WORKFLOW_NUMBER_OF_WORKERS'] ?? self::NUMBER_OR_WORKERS);

    }

    public function getWorkflowsPerRequest(): int {
        $env = getenv('WORKFLOW_WORKFLOWS_PER_REQUEST');

        return $env ?: ($_ENV['WORKFLOW_WORKFLOWS_PER_REQUEST'] ?? self::WORKFLOWS_PER_REQUEST);
    }

    public function getLogChannel(): int {
        $env = getenv('WORKFLOW_LOG_CHANNEL');

        return $env ?: ($_ENV['WORKFLOW_LOG_CHANNEL'] ?? ILogger::LOG_OFF);
    }

    /**
     * Redis stream name for supplier events
     * @return string
     */
    public function getSupplierQueue(): string {
        $env = getenv('WORKFLOW_SUPPLIER_QUEUE');

        return $env ?: ($_ENV['WORKFLOW_SUPPLIER_QUEUE'] ?? 'workflow_supplier_queue');
    }

    /**
     * Supplier cycle duration in seconds
     */
    public function getSupplierCycleDuration(): int
    {
        $env = getenv('WORKFLOW_SUPPLIER_CYCLE_DURATION');

        return $env ?: ($_ENV['WORKFLOW_SUPPLIER_CYCLE_DURATION'] ?? 30);
    }
}