CREATE TABLE IF NOT EXISTS issue_events
(
    issue_id          BIGINT PRIMARY KEY,
    matrix_event_id   TEXT        NOT NULL,
    matrix_room_id    TEXT        NOT NULL,
    reaction_event_id TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
