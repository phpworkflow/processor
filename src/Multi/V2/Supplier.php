<?php

namespace PhpWorkflow\Processor\Multi\V2;
use PhpWorkflow\Processor\Config;
use PhpWorkflow\Processor\Multi\Supplier as SupplierV1;
use Workflow\Logger\ILogger;
use PhpWorkflow\Processor\Storage\Postgres;
use Workflow\Storage\Redis\Event;
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

        $lastScheduledAt = 0;
        do {
            /**
             * @var Event[] $jobs
             */
            $jobs = $this->storage->get_workflows_for_execution($lastScheduledAt);

            $this->logger->info("SupplierV2 read " . count($jobs) . " jobs");

            foreach ($jobs as $job) {
                $this->eventsQueue->push($job);
            }

            if(count($jobs) > 0) {
                $lastScheduledAt = (int)(end($jobs)->getScheduledAt());
                continue;
            }

            $lastScheduledAt = 0;
            $this->storage->cleanup();

            sleep($this->cycleDuration);
        } while (!$this->isExit && (--$this->readCycles > 0) && $this->parentExists());

        $this->logger->info("SupplierV2 finished");
    }

    protected function parentExists() {
        $ppid = posix_getppid();
        if ($ppid == 1) {
            return false;
        }
        return posix_kill($ppid, 0);
    }
}