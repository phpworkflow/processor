<?php
namespace PhpWorkflow\Processor;

use Workflow\Event;
use Workflow\Logger\ILogger;
use Workflow\Storage\IStorage;
use Workflow\Storage\Redis\Config as RedisConfig;
use Workflow\Storage\Redis\Queue as RedisQueue;
use Workflow\Storage\Redis\Event as RedisEvent;
use Workflow\Workflow;

class Simple extends AbstractProcessor {

    protected const NUM_TASKS=100;

    private int $num_cycles=1;

    private int $sleep_time=3;

    private int $num_tasks=self::NUM_TASKS;

    protected RedisQueue $scheduleQueue;

    public function __construct(IStorage $storage, ILogger $logger)
    {
        parent::__construct($storage, $logger);
        if(!function_exists('pcntl_signal')) {
            $this->logger->info("Graceful exit not supported");
            return;
        }

        $redisCfg = new RedisConfig();
        $this->scheduleQueue = new RedisQueue([$redisCfg->scheduleQueue()]);
        $this->logger->info("Graceful exit ON");
        pcntl_async_signals(true);
        pcntl_signal(SIGHUP,  [$this, "sigHandler"]);
        pcntl_signal(SIGINT, [$this, "sigHandler"]);
        pcntl_signal(SIGTERM, [$this, "sigHandler"]);
    }

    /**
     * @param int[] $workflows
     * @return void
     */
    public function run(array $workflows = []) {
        while($this->num_cycles-- && !$this->exit) {
            $this->execute_workflows($workflows);
            sleep($this->sleep_time);
        }
    }

    /**
     * @param int[] $workflows
     * @return void
     */
    private function execute_workflows(array $workflows = []) {
        $this->logger->debug("Start");
        $wf_ids=$workflows ?: $this->storage->get_active_workflow_ids($this->num_tasks);

        $numTasks = count($wf_ids);

        if($numTasks > 0 ) {
            $this->storage->store_log("Read/recieve $numTasks task(s)");
        }

        foreach($wf_ids as $id) {
            /* @var Workflow $workflow */
            // Lock and get workflow object
            $workflow=$this->storage->get_workflow($id);
            if($workflow === null) {
                $this->logger->error("Workflow: $id was not created");
                continue; // Workflow is locked
            }

            if(!$workflow->is_finished()) {
                // Function is executed after successful event processing
                $workflow->set_sync_callback(function(Workflow $workflow, Event $event = null) {
                    if($event !== null) {
                        $this->storage->close_event($event);
                    }

                    $this->storage->save_workflow($workflow, false);
                });

                $events=$this->storage->get_events($id);
                $workflow->run($events);
            }
            // Save and unlock workflow
            $this->storage->save_workflow($workflow);

            if(!$workflow->is_finished()) {
                $job = new RedisEvent($workflow->get_id(), $workflow->get_type(), $workflow->get_start_time());
                $this->scheduleQueue->push($job);
            }

            if($this->exit) {
                return;
            }
        }

        // Run cleanup only in case if simple engine runs without supplier
        if(empty($workflows)) {
            $this->storage->cleanup();
        }

        $this->logger->debug("Finish");
    }

    public function set_params(int $num_cycles, int $sleep_time, int $num_tasks=self::NUM_TASKS): void {
        $this->num_cycles=$num_cycles;
        $this->sleep_time=$sleep_time;
        $this->num_tasks=$num_tasks;
    }

    public function sigHandler($signo)
    {
        $this->logger->info("Signal $signo. Exiting...");
        $this->exit = true;
    }
}