CREATE TABLE IF NOT EXISTS return_value (
    id UUID PRIMARY KEY DEFAULT gen_uuid_v7(),
    cooperation_lineage UUID[] NOT NULL,
    handler_name VARCHAR(255) NOT NULL,
    variable_name VARCHAR(255) NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP()
);

-- Composite unique constraint: one return value per (lineage, handler, variable_name) tuple
CREATE UNIQUE INDEX IF NOT EXISTS unique_return_value_per_lineage_handler_variable
    ON return_value (cooperation_lineage, handler_name, variable_name);

-- Index for efficient lookups by lineage (using GIN for array containment queries)
-- Used for finding child return values where child.lineage <@ parent.lineage
CREATE INDEX IF NOT EXISTS idx_return_value_cooperation_lineage
    ON return_value USING GIN (cooperation_lineage);

-- Index for lookups by variable_name (used in getReturnValues queries)
CREATE INDEX IF NOT EXISTS idx_return_value_variable_name
    ON return_value (variable_name);

-- Index for lineage cardinality + variable_name (optimizes getReturnValues query pattern)
CREATE INDEX IF NOT EXISTS idx_return_value_lineage_cardinality_variable
    ON return_value (cardinality(cooperation_lineage), variable_name);
