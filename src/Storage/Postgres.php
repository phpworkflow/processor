<?php

namespace PhpWorkflow\Processor\Storage;
use Workflow\Storage\IStorage;
use Workflow\Storage\Redis\Event as RedisEvent;

class Postgres extends \Workflow\Storage\Postgres
{
    /**
     * @param int $scheduledAt
     * @param int $limit
     * @return RedisEvent[]
     */
    public function get_workflows_for_execution(int $scheduledAt = 0, int $limit = 1000): array
    {
        $sql = <<<SQL
select workflow_id,
       type,
       scheduled_at
from (select distinct wf.workflow_id, wf.type, case when e.created_at is null then EXTRACT(EPOCH FROM wf.scheduled_at) else 0 end as scheduled_at
      from workflow wf
               left join
           event e on wf.workflow_id = e.workflow_id
      where ((e.status = :status and e.created_at <= current_timestamp)
          or
             (wf.status = :status and wf.scheduled_at <= current_timestamp))
     ) wf
where scheduled_at >= :scheduled_at
order by scheduled_at
limit :limit;
SQL;

        $statement = $this->doSql($sql, [
            'scheduled_at' => $scheduledAt,
            'status' => IStorage::STATUS_ACTIVE,
            'limit' => $limit
        ]);

        $result = [];
        while ($row = $statement->fetch()) {
            $result[$row['workflow_id']] = new RedisEvent($row['workflow_id'], $row['type'], (int)$row['scheduled_at']);
        }

        return $result;
    }
}