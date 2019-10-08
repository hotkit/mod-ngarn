CREATE TABLE IF NOT EXISTS public.mod_ngarn_migration (
    migrate_file TEXT NOT NULL,
    queue_table TEXT NOT NULL,
    PRIMARY KEY (migrate_file, queue_table)
);

/* Job table */
CREATE TABLE IF NOT EXISTS "{queue_table_schema}"."{queue_table_name}" (
    id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
    fn_name TEXT NOT NULL,
    args JSON DEFAULT '[]',
    kwargs JSON DEFAULT '{}',
    priority INTEGER DEFAULT 0,
    created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled TIMESTAMP WITH TIME ZONE,
    executed TIMESTAMP WITH TIME ZONE,
    canceled TIMESTAMP WITH TIME ZONE,
    result JSON,
    reason TEXT,
    processed_time TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS "{queue_table_schema}_{queue_table_name}_executed_idx" ON "{queue_table_schema}"."{queue_table_name}" (executed);

CREATE OR REPLACE FUNCTION "{queue_table_schema}_{queue_table_name}_notify_job"()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NOTIFY "{queue_table_schema}_{queue_table_name}";
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS "{queue_table_schema}_{queue_table_name}_notify_job_inserted" ON "{queue_table_schema}"."{queue_table_name}";
CREATE TRIGGER "{queue_table_schema}_{queue_table_name}_notify_job_inserted"
AFTER INSERT ON "{queue_table_schema}"."{queue_table_name}"
FOR EACH ROW
EXECUTE PROCEDURE "{queue_table_schema}_{queue_table_name}_notify_job"();

/* Error log table */
CREATE TABLE IF NOT EXISTS "{queue_table_schema}"."{queue_table_name}_error" (
    id TEXT NOT NULL CHECK (id !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c'),
    fn_name TEXT NOT NULL,
    args JSON DEFAULT '[]',
    kwargs JSON DEFAULT '{}',
    message TEXT NOT NULL,
    posted TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_time TEXT,
    PRIMARY KEY (id, posted)
);
