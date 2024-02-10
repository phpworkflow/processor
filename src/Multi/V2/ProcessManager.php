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
    protected const TYPE_OTHER = 'other';

    protected const MAX_EVENTS_TO_READ = 100;

    protected int $myPid;
    /**
     * @var RedisQueue
     */
    protected RedisQueue $eventsQueue;

    protected RedisLock $lock;

    protected Config $cfg;

    /**
     * @var Job[][]
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
        if (!$this->eventsQueue->isRedisConnected()) {
            $this->logger->error("No redis connection. Redis connection is mandatory for workflow processor.");
            return;
        }

        $this->logger->info("Task manager V2 ($this->myPid) started");

        while (!$this->lock->isLocked()) {
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

            if ($allowedWorkersCount <= 0) {
                continue;
            }

            $tasks = $this->selectTasksForExecution($allowedWorkersCount, $jobCfg);

            $this->startTasksExecution($tasks);

            // Update lock expire
            if (!$this->lock->isLocked()) {
                $this->isExit = true;
                $this->logger->info("Task manager ($this->myPid) lost lock. Exit.");
            }

        } while (!$this->isExit);

        $this->finalizeChildren();
    }

    protected function getJobsFromQueue(): void
    {
        $jobs = [];
        $tm = time();
        do {
            $jobs = array_merge($jobs, $this->eventsQueue->blPop(self::MAX_EVENTS_TO_READ));
        } while ($tm === time());

        $jobCfg = $this->cfg->getJobsPerWorkerCfg();

        $cnt = 0;
        foreach ($jobs as $job) {
            $wf_id = $job->getWorkflowId();
            $type = $job->getWorkflowType();

            if(!isset($jobCfg[$type])) {
                $type = self::TYPE_OTHER;
            }

            $isEvent = $job->getScheduledAt() === null;
            if ($isEvent) {
                $this->workflows[$type][$wf_id] = $job;
                $cnt++;
                continue;
            }

            $workflow = $this->workflows[$type][$wf_id] ?? null;

            $isNewScheduledWorkflow = $workflow === null || $job->getScheduledAt() > $workflow->getScheduledAt();
            if ($isNewScheduledWorkflow) {
                $this->workflows[$type][$wf_id] = $job;
                $cnt++;
            }
        }

        if ($cnt > 0) {
            $total = array_reduce($this->workflows, function ($carry, $item) {
                return $carry + count($item);
            }, 0);

            $this->logger->info("Read $cnt jobs total: $total");
        }
    }

    /**
     * @param array $workerTasks
     * @param array $batchWorkerTasks
     */
    protected function startTasksExecution(array $tasks): void
    {
        $time = time();

        foreach ($tasks as $wfIds) {
            foreach ($wfIds as $wf_id) {
                $this->taskHistory[$wf_id] = $time;
            }
            $this->newWorker($wfIds);
        }

        // Remove old tasks
        $this->taskHistory = array_filter($this->taskHistory, function ($v) use ($time) {
            return $time - $v < self::EXECUTION_PAUSE;
        });
    }

    /**
     * @param int $allowedWorkersCount
     * @param array $jobCfg
     * @return array
     */
    protected function selectTasksForExecution(int $allowedWorkersCount, array $jobCfg): array
    {
        $tasks = [];

        $readyWorkflows = $this->prepareWorkflows();

        do {
            // Filter empty arrays in $readyWorkflows
            $readyWorkflows = array_filter($readyWorkflows, static function ($v) {
                return count($v) > 0;
            });
            $types = array_keys($readyWorkflows);
            shuffle($types);

            foreach ($types as $type) {
                if($allowedWorkersCount <= 0) {
                    break;
                }

                $allowedWorkersCount--;
                $numPerWorker = (int)($jobCfg[$type] ?? 1);

                if ($numPerWorker === 1) {
                    $workflow_id = array_key_first($readyWorkflows[$type]);
                    $tasks[] = [$workflow_id];
                    unset($this->workflows[$type][$workflow_id]);
                    unset($readyWorkflows[$type][$workflow_id]);
                    continue;
                }

                $jobs = [];
                foreach ($readyWorkflows[$type] as $workflow_id => $scheduledAt) {
                    if(count($jobs) < $numPerWorker) {
                        $jobs[] = $workflow_id;
                        unset($this->workflows[$type][$workflow_id]);
                        unset($readyWorkflows[$type][$workflow_id]);
                    }
                }
                $tasks[] = $jobs;
            }

        } while ($allowedWorkersCount > 0 && !empty($readyWorkflows));

        return $tasks;
    }

    protected function prepareWorkflows(): array
    {
        $types = array_keys($this->workflows);
        $time = time();
        $readyWorkflows = [];

        // Filter workflows by scheduled time
        foreach ($types as $type) {
            foreach ($this->workflows[$type] as $wf_id => $job) {
                $lastExecTime = $this->taskHistory[$wf_id] ?? 0;
                if ($time - $lastExecTime < self::EXECUTION_PAUSE) {
                    continue;
                }

                if ($job->getScheduledAt() <= $time) {
                    $readyWorkflows[$type][$wf_id] = $job->getScheduledAt() ?: 0;
                }
            }
        }

        // Sort workflows by scheduled time
        foreach ($readyWorkflows as $type => $jobs) {
            uasort($readyWorkflows[$type], function (int $aScheduledAt, int $bScheduledAt) {
                return $aScheduledAt <=> $bScheduledAt;
            });
        }

        return $readyWorkflows;
    }
}