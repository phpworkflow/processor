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
select workflow_id, type, EXTRACT(EPOCH FROM wf.scheduled_at) as scheduled_at
    from workflow wf
        where scheduled_at >= to_timestamp(:scheduled_at)
          and wf.status = :status
          and wf.scheduled_at <= current_timestamp
            order by scheduled_at
            limit :limit;
SQL;

        $result = $this->select_workflows($sql, [
            'scheduled_at' => $scheduledAt,
            'status' => IStorage::STATUS_ACTIVE,
            'limit' => $limit
        ]);

        return $result;
    }

    public function get_workflows_with_events(int $limit = 100): array
    {
        $sql = <<<SQL
select distinct wf.workflow_id, wf.type, 0 as scheduled_at from event e left join workflow wf on e.workflow_id = wf.workflow_id
    where e.status = :status
        and e.created_at > current_timestamp - interval '4 hour'
    limit :limit;
SQL;

        $result = $this->select_workflows($sql, [
            'status' => IStorage::STATUS_ACTIVE,
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