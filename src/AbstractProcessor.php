<?php
declare(ticks=1);

namespace PhpWorkflow\Processor;

use Workflow\Logger\ILogger;
use Workflow\Storage\IStorage;

abstract class AbstractProcessor
{
    /** @var AbstractProcessor $instance */
    static protected $instance;

    /** @var IStorage $storage */
    protected $storage;

    /** @var ILogger $logger */
    protected $logger;

    /**
     * @var boolean $exit
     */
    protected $exit;

    protected function __construct(IStorage $storage, ILogger $logger)
    {
        $this->storage = $storage;
        $this->logger = $logger;
        $this->exit = false;
    }

    public static function instance(IStorage $storage, ILogger $logger)
    {
        if (empty(self::$instance)) {
            self::$instance = new static($storage, $logger);
        }
        return self::$instance;
    }

    abstract public function run(array $workflows=[]);

}