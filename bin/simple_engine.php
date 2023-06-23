<?php
require '../vendor/autoload.php';

use PhpWorkflow\Processor\Config;
use PhpWorkflow\Processor\Simple;
use Workflow\Logger\Logger;
use Workflow\Storage\Postgres;

$cfg = new Config();

$storage = Postgres::instance($cfg->getDSN());

$logger = Logger::instance($storage);

$engine = Simple::instance($storage, $logger);

$engine->set_params(10, 3); // 10 cycles of workflows execution, 3 sec between cycles
$engine->run();