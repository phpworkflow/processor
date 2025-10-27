<?php

namespace PhpWorkflow\Processor\Storage;
use PhpWorkflow\Processor\Config;
use Workflow\Storage\IStorage;
use Workflow\Storage\Postgres as Storage;
use Workflow\Storage\Redis\Event as RedisEvent;

class Postgres extends Storage
{
    protected IStorage $storage;

    public function __construct()
    {
    }

    public function cleanup(): void
    {
        $this->getStorage()->cleanup();
    }

    /**
     * @param int $scheduledAt
     * @param int $limit
     * @return RedisEvent[]
     */
    public function get_workflows_for_execution(int $scheduledAt = 0, int $limit = 1000): array
    {
        $sql = <<<SQL
SELECT workflow_id, type, EXTRACT(EPOCH FROM wf.scheduled_at) as scheduled_at
FROM workflow wf
WHERE scheduled_at >= to_timestamp(:scheduled_at)
    AND wf.status = 'ACTIVE'
    AND wf.scheduled_at <= current_timestamp
ORDER BY scheduled_at
LIMIT :limit
SQL;

        $result = $this->select_workflows($sql, [
            'scheduled_at' => $scheduledAt,
            'limit' => $limit
        ]);

        return $result;
    }

    public function get_workflows_with_events(int $limit = 100): array
    {
        $sql = <<<SQL
SELECT DISTINCT wf.workflow_id, wf.type, 0 as scheduled_at
FROM event e
LEFT JOIN workflow wf ON e.workflow_id = wf.workflow_id
WHERE e.status = 'ACTIVE'
    AND wf.workflow_id > 0
    AND wf.status = 'ACTIVE'
    AND e.created_at > current_timestamp - interval '4 hour'
LIMIT :limit
SQL;

        $result = $this->select_workflows($sql, [
            'limit' => $limit
        ]);

        return $result;
    }

    /**
     * @param string $sql
     * @param array $params
     * @return RedisEvent[]
     */
    private function select_workflows(string $sql, array $params): array
    {
        $statement = $this->getStorage()->doSql($sql, $params);
        $result = [];
        while ($row = $statement->fetch()) {
            $result[$row['workflow_id']] = new RedisEvent($row['workflow_id'], $row['type'], (int)$row['scheduled_at']);
        }
        return $result;
    }

    protected function getStorage(): IStorage
    {
        if (empty($this->storage)) {
            $this->storage = Storage::instance((new Config())->getDSN());
        }
        return $this->storage;
    }
}