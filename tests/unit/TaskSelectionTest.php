<?php

namespace unit;
use Exception;
use PHPUnit\Framework\TestCase;
use PhpWorkflow\Processor\Multi\V2\ProcessManager;
use Workflow\Storage\Redis\Event as Job;

class ProcessManagerForTesting extends ProcessManager
{
    public function __construct()
    {
    }

    public function setWorkflows(array $workflows): void
    {
        $this->workflows = $workflows;
    }

    public function prepareWorkflows(): array
    {
        return parent::prepareWorkflows();
    }

    public function selectTasksForExecution(int $allowedWorkersCount, array $jobCfg): array
    {
        return parent::selectTasksForExecution($allowedWorkersCount, $jobCfg);
    }

    public function getJobConfig(): array
    {
        $cfg = '{"Class1":1,"Class2":2,"Class3":3, "Class4":4}';
        return json_decode($cfg, true);
    }
}

class TaskSelectionTest extends TestCase {

    public function setUp(): void {
        parent::setUp();
    }

    /**
     * @return void
     * @throws Exception
     */
    function testSelectTasksForExecution(): void {
        $pm = new ProcessManagerForTesting();
        $workflows = $this->generateTasks();
        $pm->setWorkflows($workflows);
        //file_put_contents('wf.json', json_encode($workflows, JSON_PRETTY_PRINT));
        $jobCfg = $pm->getJobConfig();

        $tasks = $pm->selectTasksForExecution(10, $jobCfg);
        // file_put_contents('tasks.json', json_encode($tasks, JSON_PRETTY_PRINT));

        self::assertEquals(10, count($tasks));

        foreach ($tasks as $list) {
            $time = 0;
            foreach ($list as $workflow_id) {
                $job = $this->findJob($workflow_id, $workflows);
                self::assertNotEmpty($job);
                self::assertGreaterThanOrEqual($time, $job->getScheduledAt());
                $time = $job->getScheduledAt();
            }

        }

        self::assertIsArray($jobCfg);
        self::assertNotEmpty($jobCfg);
    }

    private function findJob($workflow_id, $workflows)
    {
        foreach ($workflows as $class => $jobs) {
            if (array_key_exists($workflow_id, $jobs)) {
                return $jobs[$workflow_id];
            }
        }
        return null;
    }

    private function generateTasks()
    {
        $class = 'Class1';
        $workflows[$class] = $this->generateTasksForClass($class, 1, 30);

        $class = 'Class2';
        $workflows[$class] = $this->generateTasksForClass($class, 2000, 50);

        $class = 'Class3';
        $workflows[$class] = $this->generateTasksForClass($class, 3000, 200);

        $class = 'Class4';
        $workflows[$class] = $this->generateTasksForClass($class, 7000, 24);

        return $workflows;
    }

    private function generateTasksForClass($class, $start_num, $count): array
    {
        $workflow_id = $start_num;
        for($i=0; $i<$count; $i++) {
            $time = time() + 300 - mt_rand(0, 1000);
            $workflow_id++;
            $tasks[$workflow_id] = new Job($workflow_id, $class, $time);
        }
        return $tasks;
    }

}
