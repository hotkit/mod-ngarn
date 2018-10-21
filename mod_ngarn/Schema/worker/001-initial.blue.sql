BEGIN;

CREATE FUNCTION url_safe(str text) RETURNS boolean AS $body$
    BEGIN
        --- Disallow back slash, forward slash, fraction slash (2044),
        --- division slash (2215), reverse solidus operator (29f5),
        --- big solidus (29f8), big reverse solidus (29f9),
        --- small reverse solidus (fe68), fullwidth solidus (ff0f),
        --- full width reverse solidus (ff3c)
        RETURN str !~ '\\|/|\u2044|\u2215|\u29f5|\u29f8|\u29f9|\ufe68|\uff0f|\uff3c';
    END
$body$ LANGUAGE plpgsql;


CREATE TABLE modngarn_job (
    id TEXT NOT NULL CHECK (url_safe(id)),
    fn_name TEXT NOT NULL,
    args JSONB DEFAULT '[]',
    kwargs JSONB DEFAULT '{}',
    priority INTEGER DEFAULT 0,
    created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled TIMESTAMP WITH TIME ZONE,
    executed TIMESTAMP WITH TIME ZONE,
    canceled TIMESTAMP WITH TIME ZONE,
    result JSONB,
    reason TEXT,
    processed_time TEXT,
    PRIMARY KEY (id)
);

CREATE INDEX idx_kwargs ON modngarn_job USING gin (kwargs);
CREATE INDEX idx_pending_jobs ON modngarn_job (executed) WHERE executed IS NULL;

COMMIT;
