<?php
require '../vendor/autoload.php';

use PhpWorkflow\Processor\Multi\ProcessManager;

$manager = new ProcessManager();

$manager->run();
