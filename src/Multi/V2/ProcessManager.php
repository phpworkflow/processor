<?php

namespace PhpWorkflow\Processor\Multi\V2;

use PhpWorkflow\Processor\Config;
use PhpWorkflow\Processor\Multi\ProcessManager as ProcessManagerV1;
use Workflow\Storage\Redis\Config as RedisConfig;
use Workflow\Storage\Redis\Queue as RedisQueue;
use Workflow\Storage\Redis\Lock as RedisLock;
use Workflow\Storage\Redis\Event as Job;

class ProcessManager extends ProcessManagerV1
{

    protected const MAX_EVENTS_TO_READ = 100;

    protected int $myPid;
    /**
     * @var RedisQueue
     */
    protected RedisQueue $eventsQueue;

    protected RedisLock $lock;

    protected Config $cfg;

    /**
     * @var Job[]
     */
    protected array $workflows = [];

    public function __construct()
    {
        parent::__construct();
        $this->myPid = getmypid();
        $redisCfg = new RedisConfig();
        $this->cfg = new Config();
        $this->eventsQueue = new RedisQueue([$redisCfg->eventsQueue(), $redisCfg->scheduleQueue(), $this->cfg->getSupplierQueue()], $redisCfg->queueLength());
        $lockValue = sprintf("%s_%s", gethostname(), $this->myPid);
        $this->lock = new RedisLock($this->cfg->getManagerLockName(), $lockValue);
    }

    public function createSupplier(): void
    {
        $this->supplier = new Supplier($this->logger);

        $this->createSupplierProcess();
    }

    public function run()
    {
        if(!$this->eventsQueue->isRedisConnected()) {
            $this->logger->error("No redis connection. Redis connection is mandatory for workflow processor.");
            return;
        }

        $this->logger->info("Task manager V2 ($this->myPid) started");

        while(!$this->lock->isLocked()) {
            if ($this->lock->lock()) {
                $this->logger->info("Task manager ($this->myPid): data input stream is locked.");
                break;
            }
            sleep(1);
        }

        $this->createSupplier();
        $jobCfg = $this->cfg->getJobsPerWorkerCfg();
        do {
            $this->getJobsFromQueue();

            $this->waitChildFinish();

            $allowedWorkersCount = $this->numWorkers - count($this->workerProcesses);

            if($allowedWorkersCount <= 0) {
                continue;
            }

            [$singleTasks, $batchTasks] = $this->selectTasksForExecution( $allowedWorkersCount, $jobCfg);

            $this->startTasksExecution($singleTasks, $batchTasks);

            // Update lock expire
            if(!$this->lock->isLocked()) {
                $this->isExit = true;
                $this->logger->info("Task manager ($this->myPid) lost lock. Exit.");
            }

        } while (!$this->isExit);

        $this->finalizeChildren();
    }

    protected function getJobsFromQueue(): void
    {
        $jobs = $this->eventsQueue->blPop(self::MAX_EVENTS_TO_READ);

        $cnt = 0;
        foreach ($jobs as $job) {
            $wf_id = $job->getWorkflowId();

            $isEvent = $job->getScheduledAt() === null;
            if( $isEvent ) {
                $this->workflows[$wf_id] = $job;
                $cnt++;
                continue;
            }

            $workflow = $this->workflows[$wf_id] ?? null;

            $isNewScheduledWorkflow = $workflow === null || $job->getScheduledAt() > $workflow->getScheduledAt();
            if( $isNewScheduledWorkflow ) {
                $this->workflows[$wf_id] = $job;
                $cnt++;
            }
        }

        if($cnt > 0 ) {
            $this->logger->info("Read $cnt jobs from queue");
        }
    }

    /**
     * @param array $workerTasks
     * @param array $batchWorkerTasks
     */
    protected function startTasksExecution(array $workerTasks, array $batchWorkerTasks): void
    {
        foreach ($workerTasks as $wf_id) {
            $this->taskHistory[$wf_id] = time();
            $this->newWorker([$wf_id]);
        }

        foreach ($batchWorkerTasks as $wfIds) {
            foreach ($wfIds as $wf_id) {
                $this->taskHistory[$wf_id] = time();
            }
            $this->newWorker($wfIds);
        }

        // Remove old tasks
        $this->taskHistory = array_filter($this->taskHistory, function ($v) {
            return time() - $v < self::EXECUTION_PAUSE;
        });
    }

    /**
     * @param int $allowedWorkersCount
     * @param array $jobCfg
     * @return array
     */
    protected function selectTasksForExecution(int $allowedWorkersCount, array $jobCfg): array
    {
        $singleTasks = [];
        $batchTasks = [];
        $readyBatchTasks = [];

        foreach ($this->workflows as $wf_id => $job) {

            // Check if tasks for workers are ready
            if (count($readyBatchTasks) + count($singleTasks) >= $allowedWorkersCount) {
                break;
            }

            // Skip jobs with scheduled time in future
            if ($job->getScheduledAt() > time()) {
                continue;
            }

            // Check if task was executed recently
            $lastExecTime = $this->taskHistory[$wf_id] ?? 0;
            if (time() - $lastExecTime < self::EXECUTION_PAUSE) {
                continue;
            }

            $jobType = $job->getWorkflowType();
            $numPerWorker = (int)($jobCfg[$jobType] ?? 1);

            // Check if only one task should be executed
            if ($numPerWorker === 1) {
                $singleTasks[] = $wf_id;
                unset($this->workflows[$wf_id]);
                continue;
            }

            // Check if number of tasks for worker is reached
            $numReady = count($batchTasks[$jobType] ?? []) ?? 0;
            if ($numReady >= $numPerWorker) {
                $readyBatchTasks[$jobType] = true;
                continue;
            }

            $batchTasks[$jobType][] = $wf_id;
            unset($this->workflows[$wf_id]);
        }

        return array($singleTasks, $batchTasks);
    }
}