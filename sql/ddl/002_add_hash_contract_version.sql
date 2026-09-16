BEGIN;

ALTER TABLE monitoring.pipeline_runs
ADD COLUMN hash_contract_version smallint;

UPDATE monitoring.pipeline_runs
SET hash_contract_version = 1
WHERE hash_contract_version IS NULL;

ALTER TABLE monitoring.pipeline_runs
ALTER COLUMN hash_contract_version SET NOT NULL;

ALTER TABLE monitoring.pipeline_runs
ADD CONSTRAINT pipeline_runs_hash_contract_version_check
CHECK (hash_contract_version > 0);

COMMIT;