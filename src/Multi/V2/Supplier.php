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
    protected const MIN_JOBS_TO_CLEANUP = 100;

    protected RedisQueue $eventsQueue;

    protected int $cycleDuration;

    protected Postgres $storage;

    protected int $lastSleepTime;

    public function __construct(ILogger $logger, $readCycles = 1000)
    {
        parent::__construct($logger, $readCycles);
        $cfg = new Config();
        $this->cycleDuration = $cfg->getSupplierCycleDuration();
        $this->eventsQueue = new RedisQueue([$cfg->getSupplierQueue()], 100000);
        $this->lastSleepTime = time();
    }

    public function run(): void
    {
        $this->storage = new Postgres();

        $lastScheduledAt = 0;

        do {
            $jobs = $this->storage->get_workflows_with_events();

            if(count($jobs) > 0) {
                $this->pushEvents($jobs);
                $this->logger->info("SupplierV2 read " . count($jobs) . " workflows with event");
                sleep(1);
            }

            /**
             * @var Event[] $jobs
             */
            $jobs = $this->storage->get_workflows_for_execution($lastScheduledAt);

            $this->logger->info("SupplierV2 read " . count($jobs) . " jobs");

            if(count($jobs) > 0) {
                $this->pushEvents($jobs);
                $lastScheduledAt = (int)(end($jobs)->getScheduledAt());
                sleep(1);
                
                if(count($jobs) > self::MIN_JOBS_TO_CLEANUP) {
                    continue;
                }
            }

            $lastScheduledAt = 0;
            $this->cleanupAndSleep();
        } while (!$this->isExit && (--$this->readCycles > 0) && $this->parentExists());

        $this->logger->info("SupplierV2 finished");
    }

    protected function pushEvents(array $jobs): void
    {
        foreach ($jobs as $job) {
            $this->eventsQueue->push($job);
        }
    }

    protected function parentExists() {
        $ppid = posix_getppid();
        if ($ppid == 1) {
            return false;
        }
        return posix_kill($ppid, 0);
    }

    /**
     * @param int $lastSleepTime
     * @return void
     */
    protected function cleanupAndSleep(): void
    {
        $sleep = $this->cycleDuration - (time() - $this->lastSleepTime);

        if($sleep > 0) {
            $this->storage->cleanup();
            sleep($sleep);
        }

        $this->lastSleepTime = time();
    }
}