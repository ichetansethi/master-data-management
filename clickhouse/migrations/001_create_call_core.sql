-- One row per call. A correction is a new row with a higher event_version.
-- ReplacingMergeTree keeps the highest version per (client_id, call_date, call_id).
-- call_date must come from call_started_at and be identical on every version of
-- an event, or a correction lands in another partition and never replaces the original.
CREATE TABLE IF NOT EXISTS call_core
(
    call_id          String,
    client_id        UInt64,
    lead_id          UUID,
    customer_id      String,
    campaign_id      LowCardinality(String),
    call_status      LowCardinality(String),
    duration_seconds UInt32,
    language         LowCardinality(String),
    call_started_at  DateTime64(3, 'UTC'),
    call_date        Date,
    event_version    UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY toYYYYMM(call_date)
ORDER BY (client_id, call_date, call_id)
