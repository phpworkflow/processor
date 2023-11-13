<?php

namespace PhpWorkflow\Processor\Multi;

use PhpWorkflow\Processor\Simple as Engine;
use PhpWorkflow\Processor\Config;
use RuntimeException;
use Workflow\Logger\ILogger;
use Workflow\Logger\Logger;
use Workflow\Storage\Postgres;

class ProcessManager
{
    private const MICRO_DELAY = 100000; // 0.1 sec

    protected const EXECUTION_PAUSE = 5; // Seconds

    private string $workflowExchangePipeName;

    protected int $numWorkers;

    protected int $supplierPid;

    /**
     * @var resource
     */
    protected $pipeFd = null;

    protected bool $isExit = false;

    protected array $workerProcesses = [];

    protected Supplier $supplier;

    protected array $taskHistory;

    protected ILogger $logger;

    protected Config $cfg;
    /**
     * @param int $numWorkers
     */
    public function __construct(int $numWorkers=0)
    {
        $this->cfg = new Config();
        $this->numWorkers = $numWorkers ?: $this->cfg->getNumberOfWorkers();
        $this->logger = Logger::instance(null, ILogger::INFO);
        $this->logger->set_log_channel($this->cfg->getLogChannel());
        $this->workflowExchangePipeName = $this->cfg->getWorkflowExchangePipeName();
        $this->taskHistory = [];

        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, [$this, 'stop']);
        pcntl_signal(SIGINT, [$this, 'stop']);
    }

    public function stop($signal)
    {
        $this->logger->info("Signal $signal -> STOP");
        $this->supplier->stop();
        $this->isExit = true;
    }

    public function run()
    {
        $mypid = getmypid();
        $this->logger->info("Task manager ($mypid) started");

        $this->createSupplier();
        do {
            $workflows = $this->getWorkflows();
            $this->createWorkerProcesses($workflows);

            if (count($this->workerProcesses) < $this->numWorkers && count($workflows) > 0) {
                continue;
            }
            sleep(1);

            $status = $this->waitChildFinish();
            if (pcntl_wifexited($status) === false) {
                $this->logger->warn('Child finished with error');
            }

        } while (!$this->isExit);

        $this->finalizeChildren();
    }

    protected function finalizeChildren(): void
    {
        while ($this->supplierPid > 0 || count($this->workerProcesses) > 0) {
            $this->waitChildFinish();
            usleep(self::MICRO_DELAY);
        }

        fclose($this->pipeFd);
    }

    protected function getWorkflows(): array
    {
        if ($this->pipeFd === null) {
            $this->pipeFd = fopen($this->workflowExchangePipeName, "rb");
            if (!$this->pipeFd) {
                throw new RuntimeException("Failed to open pipe fro reading");
            }
            stream_set_blocking($this->pipeFd, 0);
        }

        $prevPacket = false;
        // Do not block execution in this method
        $iterator = 10;
        do {
            $packet = fgets($this->pipeFd);
            if ($packet === false && $prevPacket !== false) {
                break;
            }
            $prevPacket = $packet;
            usleep(self::MICRO_DELAY);
        } while (!$this->isExit && --$iterator > 0);

        $workflows = json_decode($prevPacket, true) ?: [];
        return $workflows;
    }

    protected function createSupplier(): void
    {
        $this->supplier = new Supplier($this->logger);

        $p = $this->workflowExchangePipeName;
        if (!file_exists($p)
            && !posix_mkfifo($p, 0666)) {
            throw new RuntimeException("Failed to create named pipe ($p): " . posix_strerror(posix_get_last_error()));
        }

        $this->createSupplierProcess();
    }

    protected function createWorkerProcesses(array $workflows): void
    {
        if ($this->numWorkers === count($this->workerProcesses)) {
            return;
        }

        $this->taskHistory = array_filter($this->taskHistory, function ($v) {
            return time() - $v < self::EXECUTION_PAUSE;
        });

        $params = [];
        foreach ($workflows as $wf_id) {
            if (isset($this->taskHistory[$wf_id])) {
                continue;
            }
            $this->taskHistory[$wf_id] = time();
            $params[] = $wf_id;
        }

        if (!empty($params)) {
            $this->newWorker($workflows);
        }
    }

    protected function newWorker(array $jobs): void
    {
        $pid = pcntl_fork();

        if ($pid === 0) {
            $param = json_encode($jobs);
            $mypid = getmypid();
            $this->logger->info("Worker ($mypid): params: \"$param\"");

            $storage = Postgres::instance($this->cfg->getDSN());
            $logger = Logger::instance($storage);
            $engine = Engine::instance($storage, $logger);
            $engine->set_params(1, 0);
            $engine->run($jobs);

            exit(0);
        }

        if ($pid < 0) {
            die("Can't fork");
        }

        $this->workerProcesses[$pid] = $pid;
    }

    protected function waitChildFinish(): int
    {
        $pid = pcntl_wait($status, WNOHANG);

        if ($pid <= 0) {
            return 0;
        }

        if ($this->supplierPid === $pid) {
            $this->isExit = true;
            $this->supplierPid = 0;
            $this->logger->info("Supplier ($pid) finished with status $status");
            return $status;
        }

        $this->logger->info("Worker ($pid) finished with status $status");
        unset($this->workerProcesses[$pid]);
        return $status;
    }

    /**
     * @return void
     */
    protected function createSupplierProcess(): void
    {
        $pid = pcntl_fork();
        if ($pid < 0) {
            throw new RuntimeException("Failed to fork: " . pcntl_get_last_error());
        }

        if ($pid > 0) {
            $this->logger->info("Supplier ($pid) started");
            $this->supplierPid = $pid;
            sleep(1);
            return;
        }

        $this->logger->info("Supplier started");
        $this->supplier->run();

        exit(0);
    }

}
