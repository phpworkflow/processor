<?php
require '../vendor/autoload.php';

use PhpWorkflow\Processor\Multi\ProcessManager;

$numWorkers = 5;
$manager = new ProcessManager($numWorkers);

$manager->run();
