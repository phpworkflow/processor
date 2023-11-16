<?php

namespace PhpWorkflow\Processor\Multi\V2;
use PhpWorkflow\Processor\Config;
use PhpWorkflow\Processor\Multi\Supplier as SupplierV1;
use Workflow\Logger\ILogger;
use Workflow\Storage\Postgres;
use Workflow\Storage\Redis\Queue as RedisQueue;

class Supplier extends SupplierV1
{
    protected RedisQueue $eventsQueue;

    protected int $cycleDuration;

    public function __construct(ILogger $logger, $readCycles = 1000)
    {
        parent::__construct($logger, $readCycles);
        $cfg = new Config();
        $this->cycleDuration = $cfg->getSupplierCycleDuration();
        $this->eventsQueue = new RedisQueue([$cfg->getSupplierQueue()], 100000);
    }

    public function run(): void
    {
        $this->storage = Postgres::instance($this->cfg->getDSN());

        do {
            $jobs = $this->storage->get_scheduled_workflows();

            $this->logger->info("Supplier read " . count($jobs) . " jobs");

            foreach ($jobs as $job) {
                $this->eventsQueue->push($job);
            }

            $this->storage->cleanup();

            sleep($this->cycleDuration);
        } while (!$this->isExit && (--$this->readCycles > 0));

        $this->logger->info("Supplier finished");
    }
}