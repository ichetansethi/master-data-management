-- Checklist results, many rows per call. Latest-only: a re-analysis writes a new
-- run with a higher analysis_version. Readers must also filter on the call's
-- max(analysis_version) so a parameter dropped by a newer run disappears.
-- The consumer must write a whole run in a single INSERT.
CREATE TABLE IF NOT EXISTS call_audit_parameters
(
    call_id            String,
    client_id          UInt64,
    call_date          Date,
    audit_parameter_id LowCardinality(String),
    score              Nullable(Float32),
    normalized_score   Nullable(Float32),
    answer             String,
    justification      String,
    analysis_run_id    UUID,
    analysis_version   UInt32
)
ENGINE = ReplacingMergeTree(analysis_version)
PARTITION BY toYYYYMM(call_date)
ORDER BY (client_id, call_date, call_id, audit_parameter_id)
