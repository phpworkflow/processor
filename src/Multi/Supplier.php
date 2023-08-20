<?php

namespace PhpWorkflow\Processor\Multi;

use PhpWorkflow\Processor\Config;
use Workflow\Logger\ILogger;
use Workflow\Storage\Postgres;
use RuntimeException;

class Supplier
{
    private const DEFAULT_NUMBER_JOBS_PER_WORKER = 5;

    private const MAX_NUM_PIPE_ERRORS = 10;

    /**
     * @var Postgres
     */
    private $storage;

    /**
     * @var bool
     */
    protected bool $isExit = false;

    /**
     * @var int
     */
    protected int $readCycles;

    /**
     * @var string
     */
    public string $lastPacket = '';

    protected ILogger $logger;

    protected Config $cfg;

    public function __construct(ILogger $logger, $readCycles = 1000)
    {
        $this->logger = $logger;
        $this->cfg = new Config();
        $this->readCycles = $readCycles;
    }

    /**
     * Define the number of jobs per worker according to the workflow type
     * @return string
     */
    protected function getJobsForWorkers(): array
    {
        $cfg = $this->cfg->getJobsPerWorkerCfg();

        $workflowsPerRequest = $this->cfg->getWorkflowsPerRequest();

        $queues = $this->storage->get_active_workflow_by_type($workflowsPerRequest);
        $result = [];
        foreach ($queues as $wfType => $jobs) {
            $jobsPerWorker = $cfg[$wfType] ?? self::DEFAULT_NUMBER_JOBS_PER_WORKER;
            $chunks = array_chunk($jobs, $jobsPerWorker);
            foreach ($chunks as $c) {
                $result[] = json_encode($c) . "\n";
            }
        }

        return $result;
    }

    public function stop(): void
    {
        $this->logger->info("Supplier->stop");
        $this->isExit = true;
    }

    /**
     * @param Supplier $supplier
     */
    public function run(): void
    {
	    $pipeFd = fopen($this->cfg->getWorkflowExchangePipeName(), "wb");
	    if (!$pipeFd) {
		    throw new RuntimeException("Failed to open pipe for writing");
	    }

	    $this->storage = Postgres::instance($this->cfg->getDSN());
        $numPipeErrors = 0;

	    do {
		    $jobs = $this->getJobsForWorkers();

		    foreach ($jobs as $j) {
			    if (fwrite($pipeFd, $j) !== strlen($j)) {
				    $numPipeErrors++;
				    $this->logger->warn("$this->readCycles Can't write data to pipe");
			    }
			    $this->lastPacket = $j;
			    sleep(1);
		    }
		    sleep(1);
	    } while (!$this->isExit && (--$this->readCycles > 0) && ($numPipeErrors < self::MAX_NUM_PIPE_ERRORS));

	    fclose($pipeFd);

	    $this->logger->info("Supplier finished");
    }
}
